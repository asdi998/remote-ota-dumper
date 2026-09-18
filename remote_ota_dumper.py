#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Remote OTA Dumper —— 从 Android OTA ZIP 包（payload.bin）中快速提取分区 / 文件的
单文件工具，支持本地路径与远程 HTTP(S) URL。

文件结构（上半部分为函数接口/库 API，下半部分为 CLI，二者互不干扰）：

  1. 常量与异常
  2. ProgressUtils / ProgressStats —— 进度显示与速度统计
  3. DataFetcher    —— 数据源抽象（HTTP Range / 本地 mmap），线程安全
  4. ZipUtils       —— ZIP 结构解析：开头本地头快速定位（主）+ 中央目录（兜底）
  5. PayloadExtractor —— payload.bin 清单解析 + 分区提取引擎（两阶段流水线）
  6. FileExtractor  —— ZIP 内普通文件提取（并行分块下载）
  7. LegacyPartitions —— 旧式 recovery OTA（X.new.dat(.br)+transfer.list）分区重建
  8. ZipPayloadTool —— 高层工具类（库 API 主入口，全部状态在实例内）
  9. 模块级便捷函数  —— list_partitions / extract_partition / extract_file
  10. CLI            —— 仅当以脚本方式运行时生效

性能优化要点：
  * 定位 payload.bin 不再读文件尾部的中央目录：OTA 包中 payload.bin 的本地文件头
    位于 ZIP 开头几 KB 内（例如数据偏移 4966、头部在 ~4930），只请求开头 8KB
    顺序扫描本地文件头即可拿到数据偏移与压缩大小，1 次 HTTP 往返；窗口按
    8KB→64KB→…→4MB 指数扩容兜底，失败再回退中央目录法。
    头部缓冲在工具实例内共享：metadata 定位与内容直接复用同一缓冲，零额外请求。
  * payload_metadata.bin 是虚拟条目（无独立 ZIP 条目），与 payload.bin 同偏移，
    即其前缀（24 字节固定头 + manifest + 元数据签名），工具解析清单时已在读取；
    其索引（META-INF/com/android/metadata 的 ota-property-files，记录
    「文件名:纯数据偏移:大小」）可作为 payload.bin 的免中央目录兜底定位，
    并用于交叉校验清单长度。
  * 分区提取采用「抓取-处理」两阶段流水线：操作按 data_offset 排序并把连续操作合并
    成大块读取（请求数减少、网络顺序化）；下载线程与解压线程通过有界队列解耦；
    远程用每线程独立 Session（并发安全），本地用只读 mmap。
  * ZERO 操作不下载也不写零：输出文件先 truncate 到分区镜像大小，零区自动成为文件洞。
  * 三级重试抗抖 CDN：单次 Range 读取应用层重试（指数退避）→ 组级读取重试 →
    整次提取失败自动重试；单组失败不再导致整个分区前功尽弃。
  * 大块读取采用流式接收（128KB 粒度增量计进度）：合并请求的数量优势不变，
    但链接限速时进度条仍平滑推进，不再"一组不读完进度不动"。
  * 兼容旧式 recovery OTA（无 payload.bin）：zip 镜像文件（*.img）直接列出提取；
    X.new.dat(.br)+transfer.list 全量分区经 brotli 流式解压 + 区间重放重建，
    小米逗号格式与 AOSP 短横线格式均可（需 brotli 库）。
  * 修复：多 dst_extents 写入、压缩方法/未压缩大小的字段索引、ZIP64 条件解析
    （仅溢出字段写入扩展记录）、每块下载重复开关输出文件等原有问题。
"""

import argparse
import bz2
import hashlib
import itertools
import lzma
import math
import mmap
import os
import queue
import signal
import struct
import sys
import threading
import time
import zlib
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import zstandard

try:
    import brotli as _brotli  # 旧式 recovery OTA 的 .dat.br 分区需要
except ImportError:
    _brotli = None

import update_metadata_pb2 as um

# =====================================================================
# 1. 常量与异常
# =====================================================================

ZIP_HEADERS = {
    'END': b"\x50\x4b\x05\x06",
    'LOCAL': b"\x50\x4b\x03\x04",
    'CENTRAL': b"\x50\x4b\x01\x02",
    'END64': b"\x50\x4b\x06\x06",
    'LOCATOR64': b"\x50\x4b\x06\x07",
}

# 压缩方法 -> (名称, 解压函数)；方法 0 为未压缩
COMPRESSION_METHODS = {
    0: ("未压缩", lambda x: x),
    8: ("DEFLATE", lambda x: zlib.decompress(x, -15)),
    12: ("BZIP2", bz2.decompress),
    14: ("LZMA", lzma.decompress),
    93: ("Zstandard", zstandard.ZstdDecompressor().decompress),
    95: ("XZ", lzma.decompress),
}

HEADER_FIXED_SIZE = 24          # payload.bin 固定头大小（magic4+version8+manifest_size8+sig_size4）
CHUNK_SIZE = 1024 * 1024 * 4    # 普通文件分块下载的块大小（4MB）
MAX_RETRIES = 6                 # 单次 Range 读取的应用层重试次数（CDN 断连场景）
GROUP_READ_TRIES = 3            # 分区提取时一组数据的读取尝试次数（组级重试）
DEFAULT_THREADS = 8             # 默认并发数（限速链接可调大，GUI/CLI 均可）
__version__ = "3.3.1"
HEAD_SCAN_STEPS = (8 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024)  # 快速定位扩容窗口（8KB 起步）
HEAD_SCAN_MAX = 4 * 1024 * 1024
GROUP_TARGET = 8 * 1024 * 1024  # 合并读取的目标大小（8MB，大分区时保持大块）
GROUP_CAP = 32 * 1024 * 1024    # 单次读取上限（32MB）
MIN_GROUP_SIZE = 256 * 1024     # 自适应合并时的最小组大小（保证小分区也有并行）

# 中央目录文件头（46 字节，含签名）
_CD_STRUCT = struct.Struct("<4sHHHHHHIIIHHHHHII")
# 本地文件头（30 字节，含签名）
_LH_STRUCT = struct.Struct("<IHHHHHIIIHH")
# 共享的 Zstd 解压器（解压器无状态，可跨线程复用）
_ZSTD = zstandard.ZstdDecompressor()


class FileNotFoundInZipError(ValueError):
    """ZIP 中未找到文件"""
    pass


class PartitionNotFoundError(ValueError):
    """payload 中未找到分区"""
    pass


class DownloadInterrupted(Exception):
    """下载被中断"""
    pass


class SourceNotSupported(Exception):
    """不支持的源类型"""
    pass


# =====================================================================
# 2. 进度显示与速度统计
# =====================================================================

class ProgressUtils:
    """进度显示工具类"""

    @staticmethod
    def format_size(size):
        size = int(size)
        if size < 1024:
            return f"{size} B"
        units = ["KB", "MB", "GB", "TB", "PB"]
        shift = (size.bit_length() - 1) // 10
        shift = min(shift, len(units))
        size = f"{(size / (1 << (shift * 10))):.2f}".replace(".00", "")
        return f"{size} {units[shift - 1]}"

    @staticmethod
    def print_progress(current, total, speed=0, elapsed=0):
        percent = current * 100 / total if total else 0
        speed_str = f"{ProgressUtils.format_size(speed)}/s"
        eta = (total - current) / speed if speed > 0 else 0
        eta_str = f"{eta:.1f}s" if eta else "--"
        print(
            f"\r进度: {ProgressUtils.format_size(current)}/{ProgressUtils.format_size(total)} "
            f"({percent:.1f}%) | 速度: {speed_str} | 用时: {elapsed:.1f}s | ETA: {eta_str}",
            end="", flush=True,
        )

    @staticmethod
    def speed_from_history(history):
        """由 (时间, 累计字节) 采样列表估算速度（字节/秒）"""
        if len(history) < 2:
            return 0
        dt = history[-1][0] - history[0][0]
        if dt <= 0:
            return 0
        return int((history[-1][1] - history[0][1]) / dt)


class ProgressStats:
    """线程安全的下载进度统计（已下载字节数 + 速度采样）。

    采样按时间节流（默认 0.2s 一次），记录 (时间, 累计字节)，
    避免流式高频计数时速度估算失真。
    """

    def __init__(self, total, samples=5, sample_interval=0.2):
        self.total = total
        self.downloaded = 0
        self._lock = threading.Lock()
        self._history = deque(maxlen=samples)
        self._last_sample_t = 0.0
        self._sample_interval = sample_interval

    def add(self, n):
        now = time.time()
        with self._lock:
            self.downloaded += n
            if now - self._last_sample_t >= self._sample_interval:
                self._last_sample_t = now
                self._history.append((now, self.downloaded))

    def snapshot(self):
        with self._lock:
            return self.downloaded, list(self._history)


# =====================================================================
# 3. 数据获取 DataFetcher
# =====================================================================

class DataFetcher:
    """线程安全的 Range 读取器。

    远程源：每个线程独立 Session（requests.Session 非线程安全），
            并发请求互不干扰，且自动获得独立的 keep-alive 连接。
    本地源：打开一次并 mmap，所有读取退化为内存切片，零系统调用开销。
    """

    def __init__(self, source, threads=DEFAULT_THREADS, max_retries=MAX_RETRIES,
                 stop_event=None):
        self.source = source
        self.remote = DataFetcher.is_remote(source)
        self.max_retries = max_retries
        self.stop_event = stop_event if stop_event is not None else threading.Event()
        self._thread_local = threading.local()
        self._file = None
        self._mmap = None
        if not self.remote:
            try:
                self._file = open(source, "rb")
                self._mmap = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
            except (OSError, ValueError):
                # 空文件等无法 mmap 的情况，回退为逐次 open+seek+read
                self._mmap = None

    # ---------- 基础 ----------

    @staticmethod
    def is_remote(source):
        return source.startswith("http://") or source.startswith("https://")

    def _session(self):
        session = getattr(self._thread_local, "session", None)
        if session is None:
            retry = Retry(
                total=self.max_retries,
                backoff_factor=0.3,
                status_forcelist=[500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry, pool_maxsize=8)
            session = requests.Session()
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self._thread_local.session = session
        return session

    def read(self, start, end=None):
        """读取 [start, end]（含）之间的字节；end 为 None 时读到末尾。线程安全。"""
        if self.stop_event.is_set():
            raise DownloadInterrupted("操作已被中断")
        if self.remote:
            return self._read_remote(start, end)
        return self._read_local(start, end)

    def read_stream(self, start, end, stats):
        """流式读取 [start, end]（含），边读边把字节数计入 stats（平滑进度）。

        远程：保持单次 Range 请求（不破坏大块合并的优势），但通过流式接收
        以 128KB 粒度增量上报进度 —— 链接限速时进度条也会持续移动；
        某次尝试失败自动回滚已计字节并重试。本地：一次切片瞬时完成。
        """
        if not self.remote:
            data = self._read_local(start, end)
            stats.add(len(data))
            return data

        headers = {"Range": f"bytes={start}-{end}"}
        expected = end - start + 1
        last_error = None
        for attempt in range(self.max_retries + 1):
            if self.stop_event.is_set():
                raise DownloadInterrupted("操作已被中断")
            reported = 0
            try:
                r = self._session().get(self.source, headers=headers,
                                        timeout=(5, 60), stream=True)
                cl = r.headers.get("Content-Length")
                if r.status_code == 200 and cl is not None and int(cl) != expected:
                    r.close()
                    raise IOError("服务器未正确响应 Range 请求")
                r.raise_for_status()
                chunks = []
                for chunk in r.iter_content(chunk_size=128 * 1024):
                    if self.stop_event.is_set():
                        r.close()
                        raise DownloadInterrupted("操作已被中断")
                    if chunk:
                        chunks.append(chunk)
                        stats.add(len(chunk))
                        reported += len(chunk)
                r.close()
                return b"".join(chunks)
            except DownloadInterrupted:
                raise
            except requests.exceptions.RequestException as e:
                if self.stop_event.is_set():
                    raise DownloadInterrupted("操作已被中断") from e
                if reported:
                    stats.add(-reported)  # 回滚本次尝试的计数，避免重试重复计入
                last_error = e
                if attempt < self.max_retries:
                    time.sleep(0.3 * (2 ** attempt))
        raise IOError(f"读取远程数据失败 [{start}-{end}]: {last_error}")

    def read_head_with_size(self, length=8192):
        """读取开头 length 字节，同时从响应头取得文件总大小。

        206 响应的 Content-Range 携带总大小（RFC 要求），因此一个请求
        同时完成「取大小 + 取头部数据」，无需单独的 HEAD 请求。
        返回 (data, total_size)；服务器忽略 Range 时只取前 length 字节。
        带应用层重试（与 _read_remote 一致）：这是初始化第一步请求，
        CDN 中途断连时自动退避重试，不让一次连接抖动毁掉整个 load。
        """
        if not self.remote:
            data = self._read_local(0, length - 1)
            return data, os.path.getsize(self.source)
        headers = {"Range": f"bytes=0-{length - 1}"}
        last_error = None
        for attempt in range(self.max_retries + 1):
            if self.stop_event.is_set():
                raise DownloadInterrupted("操作已被中断")
            try:
                r = self._session().get(self.source, headers=headers,
                                        timeout=(5, 60), stream=True)
                try:
                    if r.status_code == 206:
                        cr = r.headers.get("Content-Range", "")
                        total = int(cr.rsplit("/", 1)[1]) if "/" in cr else None
                        if total is None or total <= 0:
                            raise IOError("服务器未返回 Content-Range 头")
                        chunks = []
                        got = 0
                        for chunk in r.iter_content(chunk_size=128 * 1024):
                            if self.stop_event.is_set():
                                raise DownloadInterrupted("操作已被中断")
                            if chunk:
                                chunks.append(chunk)
                                got += len(chunk)
                                if got >= length:
                                    break
                        return b"".join(chunks), total
                    if r.status_code == 200:
                        # 服务器忽略 Range：只读取前 length 字节
                        total = r.headers.get("Content-Length")
                        chunks = []
                        got = 0
                        for chunk in r.iter_content(chunk_size=128 * 1024):
                            if chunk:
                                chunks.append(chunk)
                                got += len(chunk)
                                if got >= length:
                                    break
                        if total is None:
                            raise IOError("服务器未返回 Content-Length 头")
                        return b"".join(chunks), int(total)
                    r.raise_for_status()
                    raise IOError(f"HTTP {r.status_code}")
                finally:
                    r.close()
            except DownloadInterrupted:
                raise
            except requests.exceptions.RequestException as e:
                if self.stop_event.is_set():
                    raise DownloadInterrupted("操作已被中断") from e
                last_error = e
                if attempt < self.max_retries:
                    time.sleep(0.3 * (2 ** attempt))
        raise IOError(f"读取远程文件头失败: {last_error}")

    def _read_local(self, start, end=None):
        if self._mmap is not None:
            return self._mmap[start:] if end is None else self._mmap[start:end + 1]
        with open(self.source, "rb") as f:
            f.seek(start)
            return f.read() if end is None else f.read(end - start + 1)

    def iter_range(self, start, end):
        """流式迭代 [start, end]（含）的字节块（128KB 粒度，惰性生成）。

        用于超大文件的顺序处理（如旧式 recovery OTA 的 .dat.br，可达数 GB），
        内存占用有界；远程连接中途断开时自动从已收位置续传重试，数据不重不漏。
        """
        if not self.remote:
            data = self._read_local(start, end)
            if data:
                yield data
            return
        pos = start
        expected = end - start + 1
        received = 0
        while pos <= end:
            ok = False
            last_error = None
            for attempt in range(self.max_retries + 1):
                if self.stop_event.is_set():
                    raise DownloadInterrupted("操作已被中断")
                try:
                    headers = {"Range": f"bytes={pos}-{end}"}
                    r = self._session().get(self.source, headers=headers,
                                            timeout=(5, 60), stream=True)
                    cl = r.headers.get("Content-Length")
                    if r.status_code == 200 and cl is not None \
                            and int(cl) != (end - pos + 1):
                        r.close()
                        raise IOError("服务器未正确响应 Range 请求")
                    r.raise_for_status()
                    for chunk in r.iter_content(chunk_size=128 * 1024):
                        if self.stop_event.is_set():
                            r.close()
                            raise DownloadInterrupted("操作已被中断")
                        if chunk:
                            pos += len(chunk)
                            received += len(chunk)
                            yield chunk
                    r.close()
                    ok = True
                    break
                except DownloadInterrupted:
                    raise
                except requests.exceptions.RequestException as e:
                    if self.stop_event.is_set():
                        raise DownloadInterrupted("操作已被中断") from e
                    last_error = e
                    if attempt < self.max_retries:
                        time.sleep(0.3 * (2 ** attempt))
            if not ok:
                raise IOError(f"读取远程数据失败 [{start}-{end}]: {last_error}")
            if received >= expected:
                break

    def _read_remote(self, start, end=None):
        """带应用层重试的 Range 读取（CDN 中途断连等场景，重试同一幂等范围请求）。"""
        headers = {"Range": f"bytes={start}-{end}" if end is not None else f"bytes={start}-"}
        last_error = None
        for attempt in range(self.max_retries + 1):
            if self.stop_event.is_set():
                raise DownloadInterrupted("操作已被中断")
            try:
                r = self._session().get(self.source, headers=headers, timeout=(5, 60))
                # 安全保护：服务器若忽略 Range 并返回整个大文件，会导致内存爆炸
                if r.status_code == 200 and end is not None:
                    cl = r.headers.get("Content-Length")
                    if cl is not None and int(cl) != (end - start + 1):
                        r.close()
                        raise IOError("服务器未正确响应 Range 请求")
                r.raise_for_status()
                return r.content
            except requests.exceptions.RequestException as e:
                if self.stop_event.is_set():
                    raise DownloadInterrupted("操作已被中断") from e
                last_error = e
                if attempt < self.max_retries:
                    time.sleep(0.3 * (2 ** attempt))
        raise IOError(f"读取远程数据失败 [{start}-{end}]: {last_error}")

    def file_size(self):
        """获取源文件总大小：HEAD 优先，失败用 Range bytes=0-0 的 Content-Range 兜底。"""
        if not self.remote:
            return os.path.getsize(self.source)
        try:
            r = self._session().head(self.source, allow_redirects=True, timeout=(5, 30))
            if r.status_code < 400 and "Content-Length" in r.headers:
                return int(r.headers["Content-Length"])
        except requests.exceptions.RequestException:
            pass
        r = self._session().get(self.source, headers={"Range": "bytes=0-0"}, timeout=(5, 30))
        cr = r.headers.get("Content-Range")
        if cr and "/" in cr:
            total = cr.rsplit("/", 1)[1]
            if total.isdigit():
                return int(total)
        raise ValueError("无法获取远程文件大小")

    def close(self):
        if self._mmap is not None:
            try:
                self._mmap.close()
            except Exception:
                pass
        if self._file is not None:
            try:
                self._file.close()
            except Exception:
                pass


# =====================================================================
# 4. ZIP 结构解析 ZipUtils
# =====================================================================

class ZipUtils:
    """ZIP 结构解析：快速定位（开头本地头扫描）+ 兜底（尾部中央目录）。"""

    @staticmethod
    def parse_zip64_extra(extra_field, need_uncomp=False, need_comp=False,
                          need_offset=False):
        """按需解析 ZIP64 扩展字段（ID 0x0001）。

        ZIP 规范：只有 32 位值为 0xFFFFFFFF（溢出）的字段才会写入扩展记录，
        且按固定顺序（uncomp_size → compressed_size → local_header_offset）
        排列，缺失的字段不占位。因此必须按「哪些字段溢出」条件消费，
        否则会把 offset 错读成 size（真实包如小米旧版 recovery 包只写
        溢出的 offset 一个字段）。
        """
        values = {}
        pos = 0
        while pos <= len(extra_field) - 4:
            header_id, size = struct.unpack("<HH", extra_field[pos:pos + 4])
            if header_id == 0x0001:
                data = extra_field[pos + 4:pos + 4 + size]
                ptr = 0
                for key, need in (("uncomp_size", need_uncomp),
                                  ("compressed_size", need_comp),
                                  ("local_header_offset", need_offset)):
                    if need:
                        if ptr + 8 > len(data):
                            break
                        values[key] = struct.unpack("<Q", data[ptr:ptr + 8])[0]
                        ptr += 8
                break
            pos += 4 + size
        return values

    # ---------- 快速路径：扫描 ZIP 开头的本地文件头 ----------

    @staticmethod
    def scan_head_segment(data, target, pos=0):
        """在已下载的 ZIP 开头数据中从 pos 起顺序扫描本地文件头查找目标。

        返回 (entry, pos, status)：
          entry  — 命中时为条目 dict（data_offset 直接可用）
          pos    — 扫描停止位置（扩容后可从该位置继续）
          status — 'hit' 命中 / 'need_more' 头部被窗口截断，需更多数据 /
                   'abort' 数据描述符/ZIP64 异常，不可继续（回退中央目录）/
                   'done' 窗口内扫描完毕未命中
        """
        n = len(data)
        while pos + 30 <= n:
            if data[pos:pos + 4] != ZIP_HEADERS['LOCAL']:
                pos += 1
                continue
            (_, _, flag, method, _, _, _, csize, ucsize, nlen, elen) = \
                _LH_STRUCT.unpack_from(data, pos)
            if pos + 30 + nlen > n:
                return None, pos, 'need_more'  # 文件名被窗口截断
            name = data[pos + 30:pos + 30 + nlen].decode("utf-8", "replace")
            if flag & 0x08:
                return None, pos, 'abort'  # 数据描述符：本地头中的尺寸不可信
            if pos + 30 + nlen + elen > n:
                return None, pos, 'need_more'  # 扩展字段被窗口截断
            real_csize, real_ucsize = csize, ucsize
            if csize == 0xFFFFFFFF or ucsize == 0xFFFFFFFF:
                extra = data[pos + 30 + nlen:pos + 30 + nlen + elen]
                zv = ZipUtils.parse_zip64_extra(
                    extra, need_uncomp=ucsize == 0xFFFFFFFF,
                    need_comp=csize == 0xFFFFFFFF)
                real_csize = zv.get("compressed_size", csize)
                real_ucsize = zv.get("uncomp_size", ucsize)
                if real_csize == 0xFFFFFFFF or real_ucsize == 0xFFFFFFFF:
                    return None, pos, 'abort'
            if name == target:
                return {
                    "name": name,
                    "method": method,
                    "flag": flag,
                    "compressed_size": real_csize,
                    "uncompressed_size": real_ucsize,
                    "local_offset": pos,
                    "data_offset": pos + 30 + nlen + elen,
                }, pos, 'hit'
            # 跳到下一个本地文件头（跳过本文件的数据体，payload.bin 的
            # 8GB 数据因此只消耗一次加法运算，不会逐字节扫描）
            pos += 30 + nlen + elen + real_csize
        return None, pos, 'done'

    # ---------- 兜底路径：尾部中央目录 ----------

    @staticmethod
    def find_zip_structure(fetcher, file_size):
        """读取文件尾部找 EOCD / ZIP64 EOCD，返回 (中央目录偏移, 中央目录大小)。"""
        search_end = min(1024 * 1024, file_size)
        end_chunk = fetcher.read(file_size - search_end, file_size - 1)

        locator_pos = end_chunk.rfind(ZIP_HEADERS['LOCATOR64'])
        if locator_pos != -1:
            locator_offset = file_size - search_end + locator_pos
            end_offset = struct.unpack(
                "<Q", fetcher.read(locator_offset + 8, locator_offset + 15)
            )[0]
            tail = min(end_offset + 1023, file_size - 1)
            zip64_end = fetcher.read(end_offset, tail)
            cd_offset = struct.unpack("<Q", zip64_end[48:56])[0]
            cd_size = struct.unpack("<Q", zip64_end[40:48])[0]
            return cd_offset, cd_size

        end_pos = end_chunk.rfind(ZIP_HEADERS['END'])
        if end_pos != -1:
            end_header = end_chunk[end_pos:end_pos + 22]
            cd_offset = struct.unpack("<I", end_header[16:20])[0]
            cd_size = struct.unpack("<I", end_header[12:16])[0]
            return cd_offset, cd_size
        raise ValueError("无法识别ZIP文件结构")

    @staticmethod
    def scan_central_directory(cd_data):
        """解析中央目录数据，返回条目列表（含 ZIP64 尺寸/偏移修正）。"""
        entries = []
        pos, n = 0, len(cd_data)
        while pos + 46 <= n:
            if cd_data[pos:pos + 4] != ZIP_HEADERS['CENTRAL']:
                pos += 1
                continue
            h = _CD_STRUCT.unpack_from(cd_data, pos)
            name_len, extra_len, comment_len = h[10], h[11], h[12]
            name = cd_data[pos + 46:pos + 46 + name_len].decode("utf-8", "replace")
            extra = cd_data[pos + 46 + name_len:pos + 46 + name_len + extra_len]
            zv = ZipUtils.parse_zip64_extra(
                extra,
                need_uncomp=h[9] == 0xFFFFFFFF,
                need_comp=h[8] == 0xFFFFFFFF,
                need_offset=h[16] == 0xFFFFFFFF,
            )
            entries.append({
                "name": name,
                "method": h[4],
                "flag": h[3],
                "compressed_size": zv.get("compressed_size", h[8]),
                "uncompressed_size": zv.get("uncomp_size", h[9]),
                "local_offset": zv.get("local_header_offset", h[16]),
                "data_offset": None,
            })
            pos += 46 + name_len + extra_len + comment_len
        return entries

    @staticmethod
    def resolve_data_offset(fetcher, entry):
        """中央目录条目 → 读取并校验本地头，返回数据偏移；失败走启发式搜索。"""
        local_offset = entry["local_offset"]
        try:
            header = fetcher.read(local_offset, local_offset + 29)
            if len(header) < 30 or header[:4] != ZIP_HEADERS['LOCAL']:
                raise ValueError("本地头签名无效")
            nlen = struct.unpack_from("<H", header, 26)[0]
            full = fetcher.read(local_offset, local_offset + 29 + nlen)
            name = full[30:30 + nlen].decode("utf-8", "replace")
            if name != entry["name"]:
                raise ValueError(f"本地头文件名不匹配: {name} vs {entry['name']}")
            elen = struct.unpack_from("<H", header, 28)[0]
            return local_offset + 30 + nlen + elen
        except Exception:
            return ZipUtils.heuristic_search(fetcher, local_offset, entry["name"])

    @staticmethod
    def parse_ota_property_files(metadata_text):
        """解析 META-INF/com/android/metadata 中的 ota-property-files 索引。

        OTA 包的 metadata 文本里，``ota-property-files`` 与
        ``ota-streaming-property-files`` 记录 ``文件名:数据偏移:大小`` 列表。
        注意其中的 payload_metadata.bin 是虚拟条目（无独立 ZIP 条目），
        与 payload.bin 同偏移 —— 它就是 payload.bin 的前缀
        （24 字节固定头 + manifest + 元数据签名）。返回 {文件名: (偏移, 大小)}。
        """
        result = {}
        for line in metadata_text.splitlines():
            line = line.strip()
            if not line.startswith("ota-property-files") or "=" not in line:
                continue
            value = line.split("=", 1)[1].strip().strip('"').strip("'")
            for token in value.split(","):
                parts = token.strip().split(":")
                if len(parts) == 3 and parts[1].isdigit() and parts[2].isdigit():
                    result[parts[0]] = (int(parts[1]), int(parts[2]))
        return result

    @staticmethod
    def heuristic_search(fetcher, base_offset, filename):
        """在给定偏移附近搜索目标文件的本地头（容错）"""
        search_start = max(0, base_offset - 1024)
        search_data = fetcher.read(search_start, base_offset + 1024)
        target_header = ZIP_HEADERS['LOCAL'] + filename.encode()
        found_pos = search_data.find(target_header)
        if found_pos != -1:
            return (
                search_start + found_pos + 30 + len(filename) +
                struct.unpack("<H", search_data[found_pos + 28:found_pos + 30])[0]
            )
        raise ValueError("自动修正失败，请检查ZIP文件完整性")


# =====================================================================
# 5. payload.bin 解析与分区提取 PayloadExtractor
# =====================================================================

class PayloadExtractor:
    """payload.bin（ChromeOS update_engine 格式）清单解析与分区提取引擎。"""

    @staticmethod
    def parse_payload_header(fetcher, payload_offset, file_size, read_cap=None):
        """解析 payload.bin 头部。

        read_cap：manifest 区域精确长度（metadata 的 payload_metadata.bin 大小，
        即 24 字节固定头 + manifest + 元数据签名），按需只读这么多字节，
        避免固定 512KB 的多余下载；缺省时回退 512KB。
        返回 (数据起始偏移, 分区列表, block_size, manifest 指纹)；
        manifest 指纹为清单字节的 sha256，可唯一标识该 OTA 包
        （换链接重下同包指纹不变，换包必变）。
        """
        read_len = min(read_cap or (512 * 1024), file_size - payload_offset)
        header = fetcher.read(payload_offset, payload_offset + read_len - 1)
        if len(header) < HEADER_FIXED_SIZE or header[:4] != b"CrAU":
            raise ValueError("无效的payload.bin格式")

        manifest_size = int.from_bytes(header[12:20], byteorder="big")
        metadata_sig_size = int.from_bytes(header[20:24], byteorder="big")
        partitions_start = HEADER_FIXED_SIZE + manifest_size + metadata_sig_size
        if partitions_start > len(header):
            raise ValueError("头部数据不足，无法解析清单")

        manifest = header[24:24 + manifest_size]
        dam = um.DeltaArchiveManifest()
        dam.ParseFromString(manifest)
        if not dam.partitions:
            raise ValueError("未找到有效分区")
        manifest_hash = hashlib.sha256(manifest).hexdigest()
        return partitions_start, dam.partitions, dam.block_size or 4096, manifest_hash

    @staticmethod
    def process_operation(op, data, block_size):
        """校验并解压单个操作的数据"""
        if op.data_sha256_hash:
            actual_hash = hashlib.sha256(data).digest()
            if actual_hash != op.data_sha256_hash:
                raise ValueError(f"操作 {op.data_offset} 哈希校验失败")

        if op.type == op.ZERO:
            blocks = sum(e.num_blocks for e in op.dst_extents)
            return b"\x00" * (blocks * block_size)

        decompressors = {
            op.REPLACE_XZ: lzma.decompress,
            op.ZSTD: _ZSTD.decompress,
            op.REPLACE_BZ: bz2.decompress,
            op.REPLACE: lambda x: x,
        }
        decompressor = decompressors.get(op.type)
        if decompressor is None:
            raise ValueError(f"不支持的操作类型: {op.type}")
        return decompressor(data)

    # ---------- 分区提取（两阶段流水线） ----------

    @staticmethod
    def _image_size(partition, block_size):
        """分区镜像大小：优先取清单记录值，否则按目标 extent 推算。"""
        size = partition.new_partition_info.size
        if size and size > 0:
            return size
        max_end = 0
        for op in partition.operations:
            for ext in op.dst_extents:
                max_end = max(max_end, (ext.start_block + ext.num_blocks) * block_size)
        return max_end

    @staticmethod
    def _build_groups(ops, target=GROUP_TARGET, cap=GROUP_CAP):
        """把按 data_offset 排序的操作合并成连续读组。

        组内各操作的压缩数据在源文件中物理连续，合并后一次 Range 请求
        拉整块，显著减少请求数与 RTT 开销；组大小控制在 [target, cap]。
        返回 [(组起始偏移, 组长度, [(op, 组内相对偏移, 长度), ...]), ...]
        """
        groups = []
        items = []
        gstart, glen = 0, 0
        for op in ops:
            start, length = op.data_offset, op.data_length
            if items and start == gstart + glen and glen + length <= cap:
                items.append((op, glen, length))
                glen += length
            else:
                if items:
                    groups.append((gstart, glen, items))
                items = [(op, 0, length)]
                gstart, glen = start, length
            if glen >= target:
                groups.append((gstart, glen, items))
                items, gstart, glen = [], 0, 0
        if items:
            groups.append((gstart, glen, items))
        return groups

    @staticmethod
    def _write_operation(f, op, data, block_size, lock, flush=False):
        """按目标 extent 顺序把解压数据写入输出文件（支持多 extent）。

        返回 (起始字节, 结束字节)，供上层跟踪已写区域；无 extent 时返回 None。
        flush=True 时在写锁内立即落盘（流式模式需要让其他读句柄立即可见）。
        """
        if not op.dst_extents:
            return None
        pos = 0
        start = None
        end = 0
        with lock:
            for ext in op.dst_extents:
                n = ext.num_blocks * block_size
                off = ext.start_block * block_size
                f.seek(off)
                f.write(data[pos:pos + n])
                if start is None:
                    start = off
                end = off + n
                pos += n
            if flush:
                f.flush()
        if pos != len(data):
            raise ValueError("解压数据大小与目标范围不匹配")
        return start, end

    @staticmethod
    def _split_tail_group(groups, piece):
        """把最后一个过大的组按操作拆成不超过 piece 的小组。

        目的：分区尾部剩余组少、并发低（限速 CDN 下表现为「卡 99%」），
        拆小尾部组可大幅缩短低并发阶段。返回新组列表。
        """
        if not groups:
            return groups
        gstart, glen, items = groups[-1]
        if glen <= piece:
            return groups
        head = groups[:-1]
        pieces = []
        cur_items = []
        cur_start = None
        cur_len = 0
        for op, rel, length in items:
            if cur_items and cur_len + length > piece:
                pieces.append((cur_start, cur_len, cur_items))
                cur_items = []
                cur_len = 0
                cur_start = None
            if cur_start is None:
                cur_start = gstart + rel
            # 相对偏移重定位到新组起点
            cur_items.append((op, rel - (cur_start - gstart), length))
            cur_len += length
        if cur_items:
            pieces.append((cur_start, cur_len, cur_items))
        return head + pieces

    @staticmethod
    def extract_partition(fetcher, partition, output_path, base_offset, block_size,
                          threads, on_progress=None, on_written=None):
        """提取分区镜像。

        base_offset 为 payload.bin 数据区起始（payload_offset + partitions_start）。
        ZERO 操作直接跳过（输出文件已按镜像大小 truncate，零区自动成洞）；
        其余操作按 data_offset 排序合并分组：抓取线程拉数据入有界队列，
        处理线程解压并写入，网络与 CPU 并行且互不阻塞。

        on_progress(current, total, speed, elapsed)：下载字节进度。
        on_written(prefix_bytes)：已连续写满的前缀字节数（从文件头算起，
        ZERO 区域因 truncate 已确定为零，会提前计入），可用于流式推送。
        """
        stop = fetcher.stop_event
        ops = [op for op in partition.operations
               if op.type != op.ZERO and op.data_length > 0]
        image_size = PayloadExtractor._image_size(partition, block_size)
        total = sum(op.data_length for op in ops)
        stats = ProgressStats(total)

        # 预登记 ZERO 区域：truncate 后这些区域内容确定为零，计入已写前缀
        zero_regions = []
        for op in partition.operations:
            if op.type == op.ZERO:
                for ext in op.dst_extents:
                    zero_regions.append((ext.start_block * block_size,
                                         (ext.start_block + ext.num_blocks) * block_size))

        # 已写区域跟踪：合并区间并计算从文件头开始的连续前缀
        region_lock = threading.Lock()
        regions = []
        prefix = 0

        def register_written(start, end):
            nonlocal prefix
            if start is None:
                return
            with region_lock:
                regions.append([start, end])
                regions.sort(key=lambda r: r[0])
                merged = []
                for r in regions:
                    if merged and r[0] <= merged[-1][1]:
                        merged[-1][1] = max(merged[-1][1], r[1])
                    else:
                        merged.append(r)
                regions[:] = merged
                p = 0
                for s, e in regions:
                    if s > p:
                        break
                    p = max(p, e)
                changed = True
                while changed:  # 连续前缀继续延伸进 ZERO 区域
                    changed = False
                    for s, e in zero_regions:
                        if s <= p < e:
                            p = e
                            changed = True
                if p > prefix:
                    prefix = p
                    if on_written:
                        try:
                            on_written(p)
                        except Exception:
                            pass

        with open(output_path, "w+b") as out_file:
            out_file.truncate(image_size)
            if not ops:
                return True

            ops.sort(key=lambda op: op.data_offset)
            # 自适应组大小：总数据量小时按「总量÷线程数」切分，保证小分区
            # 也有多组并行下载（CDN 常按连接限速，固定大块会让小分区退化成
            # 单连接速度）；数据量大时保持 8MB 大块（请求少、顺序访问）。
            # 若因操作粒度导致组数不足线程数，则逐次减半组目标，
            # 保证每个抓取线程都有活干（否则尾部会以低并发爬行）。
            n_fetchers = max(1, threads)
            n_waves = max(1, math.ceil(total / (n_fetchers * GROUP_TARGET)))
            group_target = max(MIN_GROUP_SIZE,
                               math.ceil(total / (n_waves * n_fetchers)))
            groups = PayloadExtractor._build_groups(ops, target=group_target)
            while len(groups) < n_fetchers and group_target > MIN_GROUP_SIZE:
                group_target = max(MIN_GROUP_SIZE, group_target // 2)
                groups = PayloadExtractor._build_groups(ops, target=group_target)
            # 尾部大组拆小：低并发阶段最多只剩 组目标÷线程数 的字节
            groups = PayloadExtractor._split_tail_group(
                groups, max(MIN_GROUP_SIZE, group_target // n_fetchers))
            work = queue.Queue(maxsize=max(2, threads * 2))
            gindex = itertools.count()
            gindex_lock = threading.Lock()
            write_lock = threading.Lock()
            n_processors = max(1, threads)

            def fetcher_worker():
                while True:
                    with gindex_lock:
                        i = next(gindex)
                    if i >= len(groups):
                        return
                    if stop.is_set():
                        return
                    gstart, glen, items = groups[i]
                    data = None
                    for attempt in range(GROUP_READ_TRIES):
                        try:
                            # 流式读取：边读边更新进度（限速时进度仍平滑推进），
                            # 失败由 read_stream 自动回滚计数并重试
                            data = fetcher.read_stream(
                                base_offset + gstart, base_offset + gstart + glen - 1,
                                stats)
                            break
                        except DownloadInterrupted:
                            return
                        except Exception as e:
                            if attempt >= GROUP_READ_TRIES - 1:
                                print(f"读取数据失败: {e}")
                                stop.set()
                                return
                            print(f"读取数据失败，重试 {attempt + 1}/{GROUP_READ_TRIES - 1}: {e}")
                            time.sleep(1.5 * (2 ** attempt))
                    for op, rel, length in items:
                        work.put((op, data[rel:rel + length]))

            def processor_worker():
                while True:
                    item = work.get()
                    if item is None:
                        work.task_done()
                        return
                    op, data = item
                    try:
                        processed = PayloadExtractor.process_operation(op, data, block_size)
                        written = PayloadExtractor._write_operation(
                            out_file, op, processed, block_size, write_lock,
                            flush=on_written is not None,
                        )
                        if written:
                            register_written(*written)
                    except Exception as e:
                        print(f"操作失败: {e}")
                        stop.set()
                    finally:
                        work.task_done()

            fetchers = [threading.Thread(target=fetcher_worker, daemon=True)
                        for _ in range(n_fetchers)]
            processors = [threading.Thread(target=processor_worker, daemon=True)
                          for _ in range(n_processors)]
            start_time = time.time()
            for t in fetchers + processors:
                t.start()
            try:
                # 阶段一：等待抓取线程结束（所有数据项都已入队或已放弃）
                while any(t.is_alive() for t in fetchers):
                    if on_progress:
                        current, history = stats.snapshot()
                        try:
                            on_progress(current, total,
                                        ProgressUtils.speed_from_history(history),
                                        time.time() - start_time)
                        except Exception:
                            pass
                    time.sleep(0.2)
                # 阶段二：每个处理线程一个哨兵，全部处理完即退出
                for _ in processors:
                    work.put(None)
                while any(t.is_alive() for t in processors):
                    if on_progress:
                        current, history = stats.snapshot()
                        try:
                            on_progress(current, total,
                                        ProgressUtils.speed_from_history(history),
                                        time.time() - start_time)
                        except Exception:
                            pass
                    time.sleep(0.2)
            finally:
                for t in fetchers + processors:
                    t.join(timeout=5)

        if on_progress:
            try:
                on_progress(total, total, 0, time.time() - start_time)
            except Exception:
                pass
        if on_written and not stop.is_set():
            try:
                on_written(image_size)
            except Exception:
                pass
        return not stop.is_set()


# =====================================================================
# 6. 普通文件提取 FileExtractor
# =====================================================================

class FileExtractor:
    """ZIP 内普通文件的提取（未压缩 → 并行直写；压缩 → 并行下载后整体解压）。"""

    @staticmethod
    def extract(fetcher, entry, output_path, threads=DEFAULT_THREADS, on_progress=None):
        if entry["method"] == 0:
            return FileExtractor._extract_stored(fetcher, entry, output_path, threads,
                                                 on_progress)
        return FileExtractor._extract_compressed(fetcher, entry, output_path, threads,
                                                 on_progress)

    @staticmethod
    def _extract_stored(fetcher, entry, output_path, threads, on_progress):
        """未压缩文件：按 4MB 块并行下载，各块独立写回（无顺序依赖）。"""
        stop = fetcher.stop_event
        file_offset = entry["data_offset"]
        file_size = entry["uncompressed_size"]
        chunk_size = min(CHUNK_SIZE, file_size) or 1
        num_chunks = max(1, math.ceil(file_size / CHUNK_SIZE))
        stats = ProgressStats(file_size)
        write_lock = threading.Lock()
        start_time = time.time()

        def fetch(i):
            if stop.is_set():
                return None
            start = i * chunk_size
            end = min(start + chunk_size, file_size)
            if start >= file_size:
                return None
            return i, fetcher.read(file_offset + start, file_offset + end - 1)

        with open(output_path, "w+b") as f:
            f.truncate(file_size)
            with ThreadPoolExecutor(max_workers=threads,
                                    thread_name_prefix="FileFetch") as executor:
                futures = [executor.submit(fetch, i) for i in range(num_chunks)]
                for future in as_completed(futures):
                    if stop.is_set():
                        break
                    result = future.result()
                    if result is None:
                        continue
                    i, data = result
                    with write_lock:
                        f.seek(i * chunk_size)
                        f.write(data)
                    stats.add(len(data))
                    if on_progress:
                        current, history = stats.snapshot()
                        try:
                            on_progress(current, file_size,
                                        ProgressUtils.speed_from_history(history),
                                        time.time() - start_time)
                        except Exception:
                            pass

        if stop.is_set():
            return False
        if on_progress:
            try:
                on_progress(file_size, file_size, 0, time.time() - start_time)
            except Exception:
                pass
        return True

    @staticmethod
    def _extract_compressed(fetcher, entry, output_path, threads, on_progress):
        """压缩文件：并行下载压缩数据（顺序组装）→ 单次解压 → 写入。"""
        stop = fetcher.stop_event
        file_offset = entry["data_offset"]
        compressed_size = entry["compressed_size"]
        uncompressed_size = entry["uncompressed_size"]
        method = entry["method"]
        info = COMPRESSION_METHODS.get(method)
        if info is None:
            raise ValueError(f"不支持的压缩方法: {method}")
        decompressor = info[1]

        chunk_size = min(CHUNK_SIZE, compressed_size) or 1
        num_chunks = max(1, math.ceil(compressed_size / CHUNK_SIZE))
        results = {}
        stats = ProgressStats(compressed_size)
        start_time = time.time()

        def fetch(i):
            if stop.is_set():
                return None
            start = i * chunk_size
            end = min(start + chunk_size, compressed_size)
            if start >= compressed_size:
                return None
            return i, fetcher.read(file_offset + start, file_offset + end - 1)

        with ThreadPoolExecutor(max_workers=threads,
                                thread_name_prefix="FileFetch") as executor:
            futures = [executor.submit(fetch, i) for i in range(num_chunks)]
            for future in as_completed(futures):
                if stop.is_set():
                    break
                result = future.result()
                if result is None:
                    continue
                i, data = result
                results[i] = data
                stats.add(len(data))
                if on_progress:
                    current, history = stats.snapshot()
                    try:
                        on_progress(current, compressed_size,
                                    ProgressUtils.speed_from_history(history),
                                    time.time() - start_time)
                    except Exception:
                        pass

        if stop.is_set():
            return False

        compressed = b"".join(results[i] for i in range(num_chunks))
        data = decompressor(compressed)
        if len(data) != uncompressed_size:
            raise ValueError(f"解压后大小不匹配: 预期 {uncompressed_size}, 实际 {len(data)}")
        with open(output_path, "wb") as f:
            f.write(data)

        if on_progress:
            try:
                on_progress(uncompressed_size, uncompressed_size, 0,
                            time.time() - start_time)
            except Exception:
                pass
        return True


# =====================================================================
# 7. 旧式 recovery OTA 分区重建（LegacyPartitions）
#    适用 Android <10 的 system.new.dat(.br) + system.transfer.list 全量包
# =====================================================================

LEGACY_BLOCK = 4096  # transfer list 的块大小固定为 4096


class LegacyPartitions:
    """旧式 recovery OTA（X.new.dat / X.new.dat.br + X.transfer.list）支持。

    仅支持全量包（命令为 new/zero/erase，patch.dat 为空）；
    增量包（含 diff/move/stash 命令）需旧镜像，抛错明确提示。
    """

    @staticmethod
    def parse_transfer_list(text):
        """解析 transfer list，兼容小米逗号格式与 AOSP 短横线格式。

        返回 (镜像块数, [(类型, [(起块, 止块), ...]), ...]，块区间为止开区间)。
        类型 ∈ {new, zero, erase}；出现其它命令抛 LegacyDatError。
        """
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        header_blocks = 0
        commands = []
        for idx, line in enumerate(lines):
            if idx == 0:
                continue  # 版本号
            if not line or line[0].isdigit():
                # 纯数字行：小米格式为「总块数,0,0」；AOSP 格式为「命令条数」
                try:
                    header_blocks = max(header_blocks, int(line.split()[0]))
                except ValueError:
                    pass
                continue
            word = line.split(None, 1)[0]
            cmd_type = word.lower()
            if cmd_type not in ("new", "zero", "erase"):
                raise ValueError(
                    f"transfer list 含增量命令 '{cmd_type}'（需要旧镜像），"
                    f"仅支持全量包")
            # 拆 token：小米逗号格式（'new 4,0,471,...'）或 AOSP 空格格式
            # （'new 4 0-471 ...'），统一先把空白归一为逗号
            tokens = [t for t in line.replace(" ", ",").split(",") if t]
            try:
                count = int(tokens[1])
                raw_values = tokens[2:]
            except (IndexError, ValueError):
                raise ValueError(f"无法解析 transfer list 行: {line[:80]}")
            if count != len(raw_values):
                raise ValueError(f"transfer list 行数值个数不符: {line[:80]}")
            if any("-" in v for v in raw_values):
                # AOSP 风格：每个 token 即一个 'start-end' 区间
                ranges = []
                for v in raw_values:
                    s, e = v.split("-", 1)
                    ranges.append((int(s), int(e)))
            else:
                # 小米风格：数值两两成对 (起块, 止块)
                if count % 2 != 0:
                    raise ValueError(f"transfer list 行区间数不符: {line[:80]}")
                values = [int(v) for v in raw_values]
                ranges = [(values[i], values[i + 1])
                          for i in range(0, count, 2)]
            commands.append((cmd_type, ranges))
        # 镜像块数：头部数字行（小米=总块数）与区间最大止块的较大者
        # （AOSP 头部是命令条数，此时区间止块为准）
        max_block = max((e for _, ranges in commands for _, e in ranges),
                        default=0)
        return max(header_blocks, max_block), commands

    @staticmethod
    def total_new_blocks(commands):
        return sum(e - s for cmd, ranges in commands if cmd == "new"
                   for s, e in ranges)

    @staticmethod
    def extract(fetcher, data_entry, list_text, output_path, on_progress=None):
        """下载 X.new.dat(.br) 并重建分区镜像。返回 True。

        data_entry：zip 中 .new.dat / .new.dat.br 条目的 CD 信息；
        数据流按 transfer list 中 new 区间的顺序解压并写盘，
        最终字节数与区间总长严格比对（不一致即解析/传输错误）。
        """
        stop = fetcher.stop_event
        header_blocks, commands = LegacyPartitions.parse_transfer_list(list_text)
        segments = [(s * LEGACY_BLOCK, e * LEGACY_BLOCK)
                    for cmd, ranges in commands if cmd == "new"
                    for s, e in ranges]
        if not segments:
            raise ValueError("transfer list 中没有 new 命令")
        max_block = max(e for cmd, ranges in commands for s, e in ranges)
        image_size = max(header_blocks, max_block) * LEGACY_BLOCK
        data_size = data_entry["compressed_size"]
        name = data_entry["name"]
        is_br = name.lower().endswith(".br")
        if is_br and _brotli is None:
            raise ValueError("需要 brotli 库支持 .dat.br 分区：pip install brotli")
        if data_entry["method"] != 0:
            raise ValueError(f"{name} 的 zip 压缩方法为 "
                             f"{COMPRESSION_METHODS.get(data_entry['method'], ('未知',))[0]}，"
                             f"旧式分区要求 ZIP_STORED")

        decompressor = _brotli.Decompressor() if is_br else None
        start_time = time.time()
        total_download = data_size
        downloaded = 0

        def progress_cb(n):
            nonlocal downloaded
            downloaded += n
            if on_progress:
                try:
                    on_progress(downloaded, total_download,
                                ProgressUtils.speed_from_history(
                                    [(time.time(), downloaded)]),
                                time.time() - start_time)
                except Exception:
                    pass

        with open(output_path, "w+b") as out:
            out.truncate(image_size)
            seg_idx = 0
            seg_pos = segments[0][0] if segments else None
            buf = b""
            for chunk in fetcher.iter_range(
                    data_entry["data_offset"],
                    data_entry["data_offset"] + data_size - 1):
                progress_cb(len(chunk))
                if stop.is_set():
                    raise DownloadInterrupted("操作已被中断")
                if decompressor is not None:
                    buf += decompressor.process(chunk)
                else:
                    buf += chunk
                while buf and seg_idx < len(segments):
                    s, e = segments[seg_idx]
                    take = min(len(buf), e - seg_pos)
                    out.seek(seg_pos)
                    out.write(buf[:take])
                    buf = buf[take:]
                    seg_pos += take
                    if seg_pos >= e:
                        seg_idx += 1
                        if seg_idx < len(segments):
                            seg_pos = segments[seg_idx][0]
                if seg_idx >= len(segments) and buf and not stop.is_set():
                    raise ValueError("数据长度超过 transfer list 声明")
            if decompressor is not None and not decompressor.is_finished():
                # 还有缓冲输出或输入被截断：再取一次输出并校验
                tail = decompressor.process(b"")
                if tail:
                    buf = tail + buf
                    while buf and seg_idx < len(segments):
                        s, e = segments[seg_idx]
                        take = min(len(buf), e - seg_pos)
                        out.seek(seg_pos)
                        out.write(buf[:take])
                        buf = buf[take:]
                        seg_pos += take
                        if seg_pos >= e:
                            seg_idx += 1
                            if seg_idx < len(segments):
                                seg_pos = segments[seg_idx][0]
                if not decompressor.is_finished():
                    raise ValueError("brotli 流未完整结束（数据被截断？）")
                if buf:
                    raise ValueError("数据长度超过 transfer list 声明")
            if seg_idx != len(segments):
                raise ValueError(
                    "数据长度不足 transfer list 声明（镜像或解析不匹配）")
        if on_progress:
            try:
                on_progress(total_download, total_download, 0,
                            time.time() - start_time)
            except Exception:
                pass
        return True


# =====================================================================
# 8. 高层工具类 ZipPayloadTool（库 API 主入口）
# =====================================================================

class ZipPayloadTool:
    """打开一个 ZIP 源（URL 或本地路径）并执行分区/文件操作。

    用法::

        with ZipPayloadTool("update.zip", threads=8) as tool:
            parts = tool.list_partitions()
            tool.extract_partition("boot", "boot.img")
            tool.extract_file("hello.txt", "hello.txt")

    所有状态（Session、缓存、中断标志）都在实例内，可安全重复调用，
    也可并行创建多个实例处理不同源。中断调用 ``tool.stop()``。
    """

    def __init__(self, source, threads=DEFAULT_THREADS, max_retries=MAX_RETRIES,
                 on_progress=None):
        self.source = source
        self.threads = threads
        self.on_progress = on_progress
        self.stop_event = threading.Event()
        self.fetcher = DataFetcher(source, threads=threads, max_retries=max_retries,
                                   stop_event=self.stop_event)
        self.file_size = None
        self._cd_offset = None
        self._cd_size = None
        self._cd_entries = None
        self._head_entries = {}     # 头部快速定位缓存: 名称 -> 条目
        self._head_data = b""       # 共享头部缓冲（8KB 起步扩容，跨目标复用，
                                    # payload.bin 与 metadata 定位只取一次头部数据）
        self._payload_offset = None
        self._payload_size = None
        self._partitions_start = None
        self._partitions = None
        self._block_size = None
        self.payload_metadata_size = None  # 来自 ota-property-files 的 payload_metadata.bin 大小
        self.metadata_warning = None       # 定位结果与索引不一致时的警告文本
        self._manifest_hash = None         # payload manifest 指纹（包唯一标识）
        self._cd_fingerprint = None        # 中央目录指纹（兼容模式包的标识）

    # ---------- payload 内部信息（只读） ----------
    # 供需要操作级访问的调用方（如 web 服务的内核版本提取）使用，
    # 调用 list_partitions() 后即可读取。

    @property
    def payload_offset(self):
        return self._payload_offset

    @property
    def payload_size(self):
        return self._payload_size

    @property
    def partitions_start(self):
        return self._partitions_start

    @property
    def partitions(self):
        return self._partitions

    @property
    def block_size(self):
        return self._block_size

    @property
    def package_id(self):
        """当前包的唯一指纹。

        A/B 包：payload manifest 的 sha256；
        兼容模式（无 payload.bin 的 recovery 包）：中央目录的 sha256。
        换链接重下同一个包指纹不变；换包（哪怕分区大小相同）指纹必变，
        可用于区分本地已下载文件是否属于当前包。调用 list_partitions() 后可用。
        """
        return self._manifest_hash or self._cd_fingerprint

    # ---------- 生命周期 ----------

    def load(self):
        """源可用性检查与初始化（所有操作前自动调用，也可手动调用）。

        远程源：1 次「开头 8KB」请求 —— 响应头携带文件总大小
        （206 Content-Range），头部数据同时作为共享头部缓冲，
        之后定位 metadata/payload.bin 零额外请求。本地源：取文件大小。
        """
        if self.file_size is not None:
            return True
        if self.fetcher.remote:
            data, size = self.fetcher.read_head_with_size(HEAD_SCAN_STEPS[0])
            self.file_size = size
            if not self._head_data:
                self._head_data = data
        else:
            self.file_size = self.fetcher.file_size()
        return True

    def stop(self):
        self.stop_event.set()

    def close(self):
        self.fetcher.close()

    def __enter__(self):
        self.load()
        return self

    def __exit__(self, *exc_info):
        self.close()

    # ---------- 条目定位 ----------

    # 权威确认无 payload.bin 的哨兵：metadata 的 ota-property-files 存在
    # 但未登记 payload.bin（旧式 recovery/BLOCK 包），可直接短路，
    # 避免徒劳的 4MB 头部扩容与中央目录读取
    _PAYLOAD_ABSENT = object()

    def _locate_entry(self, name):
        """定位文件条目：payload.bin 先走 ota-property-files 索引（头部缓冲内的
        metadata，零额外请求），其余文件走本地头扫描；都不中再回退中央目录。"""
        if name in self._head_entries:
            return self._head_entries[name]
        self.load()
        entry = None
        if name == "payload.bin":
            # metadata 的 ota-property-files 直接给出 payload.bin 纯数据偏移与
            # payload_metadata.bin 大小（manifest 区域精确长度）
            found = self._locate_payload_via_metadata()
            if found is ZipPayloadTool._PAYLOAD_ABSENT:
                # 索引权威确认无 payload.bin：仅做缓冲内 + 一次 64KB 的
                # 防御性扫描（防非常规打包遗漏登记），然后直接判定不存在
                try:
                    entry = self._locate_in_head(name, grow_steps=2)
                except DownloadInterrupted:
                    raise
                except Exception:
                    entry = None
                if entry is not None:
                    self._head_entries[name] = entry
                    return entry
                return None
            entry = found
        if entry is None:
            try:
                entry = self._locate_in_head(name)
            except DownloadInterrupted:
                raise
            except Exception:
                entry = None
        if entry is not None:
            self._head_entries[name] = entry
            return entry

        entries = self._load_cd_entries()
        if entries is None:
            return None
        for e in entries:
            if e["name"] == name:
                try:
                    e["data_offset"] = ZipUtils.resolve_data_offset(self.fetcher, e)
                except DownloadInterrupted:
                    raise
                except Exception:
                    continue
                return e
        return None

    def _locate_in_head(self, name, grow_steps=None):
        """共享头部缓冲快速定位：首次只取开头 8KB，扫描需要时才扩容。

        缓冲在整个 ZipPayloadTool 生命周期内复用：定位 payload.bin 下载的
        头部数据，后续定位 META-INF/com/android/metadata（get_ota_info）
        直接命中同一缓冲，零额外请求。grow_steps 限制扩容次数（默认不限）。
        """
        pos = 0
        if not self._head_data:
            target_len = min(HEAD_SCAN_STEPS[0], self.file_size)
            self._head_data = self.fetcher.read(0, target_len - 1)
        max_steps = len(HEAD_SCAN_STEPS) if grow_steps is None else grow_steps
        for step in range(1, max_steps + 1):
            entry, pos, status = ZipUtils.scan_head_segment(self._head_data, name, pos)
            if status == 'hit':
                # 条目数据若已完整落在头部缓冲内（如开头的小文件 metadata），
                # 直接缓存内容，读取时零额外请求
                if entry["data_offset"] + entry["compressed_size"] <= len(self._head_data):
                    entry["_content"] = bytes(
                        self._head_data[entry["data_offset"]:
                                        entry["data_offset"] + entry["compressed_size"]])
                return entry
            if status == 'abort':
                return None  # 数据描述符等 → 回退中央目录
            # need_more / done：当前窗口未命中 → 按步长扩容后继续
            if len(self._head_data) >= min(self.file_size, HEAD_SCAN_MAX) \
                    or step >= max_steps:
                return None
            target_len = min(len(self._head_data) + HEAD_SCAN_STEPS[step],
                             self.file_size)
            if target_len > len(self._head_data):
                chunk = self.fetcher.read(len(self._head_data), target_len - 1)
                self._head_data += chunk
        return None

    def _read_entry_content(self, entry):
        """读取条目完整内容（支持未压缩 / DEFLATE）；头部缓冲已缓存则直接取用。"""
        data = entry.get("_content")
        if data is None:
            data = self.fetcher.read(entry["data_offset"],
                                     entry["data_offset"] + entry["compressed_size"] - 1)
        if entry["method"] == 0:
            return data
        if entry["method"] == 8:
            return zlib.decompress(data, -15)
        raise ValueError(f"不支持的压缩方法: {entry['method']}")

    def _locate_payload_via_metadata(self):
        """解析 META-INF/com/android/metadata 的 ota-property-files 定位 payload.bin。

        返回：
          条目 dict —— payload.bin 被登记（偏移/大小可用，纯数据起始）；
          _PAYLOAD_ABSENT —— metadata 存在且含 property-files 索引，但未登记
                            payload.bin（旧式 recovery/BLOCK 包），权威确认不存在；
          None —— metadata 缺失或没有 property-files（无法判定，走其他路径）。
        """
        try:
            meta = self._locate_entry("META-INF/com/android/metadata")
            if meta is None:
                return None
            text = self._read_entry_content(meta).decode("utf-8", "replace")
            has_props = ("ota-property-files" in text
                         or "ota-streaming-property-files" in text)
            props = ZipUtils.parse_ota_property_files(text)
            if "payload.bin" not in props:
                if has_props:
                    return ZipPayloadTool._PAYLOAD_ABSENT
                return None
            offset, size = props["payload.bin"]
            if not (0 < offset < self.file_size and offset + size <= self.file_size):
                return None
            meta_size = props.get("payload_metadata.bin", (None, None))[1]
            self.payload_metadata_size = meta_size
            return {
                "name": "payload.bin",
                "method": 0,  # 流式 OTA 的 payload.bin 恒为 ZIP_STORED
                "flag": 0,
                "compressed_size": size,
                "uncompressed_size": size,
                "local_offset": None,
                "data_offset": offset,
            }
        except DownloadInterrupted:
            raise
        except Exception:
            return None

    def _load_cd_entries(self):
        if self._cd_entries is not None:
            return self._cd_entries
        if self._cd_offset is None:
            try:
                self._cd_offset, self._cd_size = ZipUtils.find_zip_structure(
                    self.fetcher, self.file_size
                )
            except Exception:
                return None
        try:
            cd_data = self.fetcher.read(self._cd_offset, self._cd_offset + self._cd_size - 1)
            self._cd_fingerprint = hashlib.sha256(cd_data).hexdigest()
            self._cd_entries = ZipUtils.scan_central_directory(cd_data)
        except DownloadInterrupted:
            raise
        except Exception:
            self._cd_entries = []
        return self._cd_entries

    # ---------- 文件操作 ----------

    def list_files(self):
        """返回 ZIP 内全部文件名列表；中央目录不可用时返回 None。"""
        entries = self._load_cd_entries()
        if entries is None:
            return None
        return [e["name"] for e in entries]

    def extract_file(self, name, output=None):
        """提取 ZIP 内任意文件，成功返回 True。文件不存在抛 FileNotFoundInZipError。"""
        if output is None:
            output = os.path.basename(name.replace("\\", "/"))
        entry = self._locate_entry(name)
        if entry is None:
            raise FileNotFoundInZipError(f"ZIP中未找到 {name}")
        try:
            return FileExtractor.extract(self.fetcher, entry, output,
                                         self.threads, self.on_progress)
        except DownloadInterrupted:
            self._remove_partial(output)
            return False

    def _remove_partial(self, path):
        try:
            os.remove(path)
        except OSError:
            pass

    # ---------- payload 操作 ----------

    def _load_payload_info(self):
        """定位并解析 payload.bin（结果缓存）。成功返回 True。"""
        if self._partitions is not None:
            return True
        self.load()
        entry = self._locate_entry("payload.bin")
        if entry is None:
            return False
        self._payload_offset = entry["data_offset"]
        self._payload_size = entry["uncompressed_size"]
        try:
            (self._partitions_start, self._partitions, self._block_size,
             self._manifest_hash) = PayloadExtractor.parse_payload_header(
                self.fetcher, self._payload_offset, self.file_size,
                read_cap=self.payload_metadata_size,  # 精确读取 manifest 区域
            )
        except Exception:
            return False
        # payload_metadata.bin 大小 = 24 字节固定头 + manifest + 元数据签名，
        # 应与解析出的数据区起始偏移一致（不一致说明定位可能出错）
        if self.payload_metadata_size and self.payload_metadata_size != self._partitions_start:
            self.metadata_warning = (
                f"ota-property-files 记录的 payload_metadata 大小 "
                f"({self.payload_metadata_size}) 与清单解析结果 "
                f"({self._partitions_start}) 不一致"
            )
        return True

    def list_partitions(self):
        """返回分区信息列表 ``[{name, image_size, download_size, kind}, ...]``。

        kind 为 "payload"（A/B payload.bin 分区）或 "zip_img"（兼容模式：
        旧式 recovery/BLOCK 包中 zip 根目录与 firmware-update/ 下的 *.img 镜像）。
        无 payload.bin 也无镜像文件时返回 None。
        image_size 与提取产物实际大小一致（清单未记录大小时按 extent 推算）。"""
        if self._load_payload_info():
            return [{
                "name": p.partition_name,
                "image_size": PayloadExtractor._image_size(p, self._block_size),
                "download_size": sum(op.data_length for op in p.operations),
                "kind": "payload",
            } for p in self._partitions]
        return self._compat_partitions()

    def get_ota_info(self):
        """读取并解析 META-INF/com/android/metadata，返回键值字典。

        典型键：pre-device（设备代号）、post-build / post-build-incremental（版本）、
        post-sdk-level（API 级别）、post-security-patch-level（安全补丁）、
        post-timestamp（Unix 时间戳）、ota-type（AB/A）等；
        无该文件或解析失败返回 None。ota-property-files 这类超长索引键会跳过。
        """
        try:
            entry = self._locate_entry("META-INF/com/android/metadata")
            if entry is None:
                return None
            text = self._read_entry_content(entry).decode("utf-8", "replace")
        except Exception:
            return None
        info = {}
        for line in text.splitlines():
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if "property-files" in key or key in info:
                continue
            info[key] = value.strip().strip('"').strip("'")
        return info or None

    def _compat_partitions(self):
        """兼容模式：无 payload.bin 的旧式 recovery 包。

        - kind "zip_img"：zip 根目录与 firmware-update/ 下的 *.img 镜像
          （boot.img、dtbo.img…，直接下载）
        - kind "legacy_dat"：system.new.dat(.br) + transfer.list 的全量分区
          （brotli 解压 + 区间重放重建，见 LegacyPartitions）
        返回列表或 None。
        """
        entries = self._load_cd_entries()
        if not entries:
            return None
        by_name = {e["name"]: e for e in entries}
        parts = []
        seen = set()
        for e in entries:
            raw_name = e["name"]
            base = None
            if "/" not in raw_name:
                base = raw_name
            elif raw_name.startswith("firmware-update/"):
                base = raw_name.rsplit("/", 1)[1]
            if base and base.lower().endswith(".img"):
                pname = base[:-4]
                parts.append({
                    "name": pname,
                    "image_size": e["uncompressed_size"],
                    "download_size": e["compressed_size"],
                    "kind": "zip_img",
                    "entry": raw_name,
                })
                seen.add(pname)
            # 旧式 dat 分区（根目录 *.new.dat / *.new.dat.br）
            if "/" in raw_name:
                continue
            if raw_name.endswith(".new.dat.br"):
                pname = raw_name[:-len(".new.dat.br")]
            elif raw_name.endswith(".new.dat"):
                pname = raw_name[:-len(".new.dat")]
            else:
                continue
            if pname in seen:
                continue
            seen.add(pname)
            image_size = 0
            list_name = pname + ".transfer.list"
            if list_name in by_name:
                try:
                    list_entry = self._locate_entry(list_name)
                    if list_entry is not None:
                        text = self._read_entry_content(list_entry) \
                            .decode("utf-8", "replace")
                        blocks, _cmds = LegacyPartitions.parse_transfer_list(text)
                        image_size = blocks * LEGACY_BLOCK
                except Exception:
                    image_size = 0
            parts.append({
                "name": pname,
                "image_size": image_size,
                "download_size": e["compressed_size"],
                "kind": "legacy_dat",
                "entry": raw_name,
            })
        return parts or None

    def extract_partition(self, name, output=None, on_written=None):
        """提取指定分区为镜像文件，成功返回 True。
        分区不存在抛 PartitionNotFoundError；默认输出 ``name.img``。

        无 payload.bin 时自动进入兼容模式：从 zip 镜像文件（boot.img、
        firmware-update/*.img 等）提取对应分区。
        on_written：可选回调 (已写连续前缀字节数)，用于流式场景。
        """
        if output is None:
            output = name + ".img"
        if not self._load_payload_info():
            # 兼容模式：recovery/BLOCK 包镜像文件
            for candidate in (name, name + ".img"):
                try:
                    entry = self._locate_entry(candidate)
                except DownloadInterrupted:
                    raise
                if entry is None:
                    continue
                try:
                    ok = FileExtractor.extract(self.fetcher, entry, output,
                                               self.threads, self.on_progress)
                    if not ok:
                        self._remove_partial(output)
                    return ok
                except DownloadInterrupted:
                    self._remove_partial(output)
                    return False
                except Exception:
                    self._remove_partial(output)
                    raise
            # 兼容模式：旧式 dat 分区（X.new.dat / X.new.dat.br + transfer.list）
            for suffix in (".new.dat.br", ".new.dat"):
                try:
                    data_entry = self._locate_entry(name + suffix)
                except DownloadInterrupted:
                    raise
                if data_entry is None:
                    continue
                try:
                    list_entry = self._locate_entry(name + ".transfer.list")
                    if list_entry is None:
                        raise ValueError(f"缺少 {name}.transfer.list")
                    list_text = self._read_entry_content(list_entry) \
                        .decode("utf-8", "replace")
                    ok = LegacyPartitions.extract(
                        self.fetcher, data_entry, list_text, output,
                        self.on_progress)
                    if not ok:
                        self._remove_partial(output)
                    return ok
                except DownloadInterrupted:
                    self._remove_partial(output)
                    return False
                except Exception:
                    self._remove_partial(output)
                    raise
            raise PartitionNotFoundError(
                f"未找到分区 '{name}'（该包无 payload.bin，也没有对应的镜像文件）")
        target = next((p for p in self._partitions if p.partition_name == name), None)
        if target is None:
            available = ", ".join(p.partition_name for p in self._partitions)
            raise PartitionNotFoundError(f"未找到分区 '{name}'，可用分区: {available}")
        try:
            ok = PayloadExtractor.extract_partition(
                self.fetcher, target, output,
                self._payload_offset + self._partitions_start,
                self._block_size, self.threads, self.on_progress, on_written,
            )
            if not ok:
                # 失败（网络/校验/中断）时清理半成品，避免留下看似完整
                # 实则残缺的镜像（半成品经 truncate 后大小与完整镜像相同）
                self._remove_partial(output)
            return ok
        except DownloadInterrupted:
            self._remove_partial(output)
            return False
        except Exception:
            self._remove_partial(output)
            raise


# =====================================================================
# 9. 模块级便捷函数（函数接口）
# =====================================================================

def list_partitions(source, threads=DEFAULT_THREADS, on_progress=None):
    """列出 OTA 分区信息。

    :param source: ZIP 文件 URL 或本地路径
    :return: ``[{name, image_size, download_size}, ...]``；无 payload.bin 返回 None
    """
    with ZipPayloadTool(source, threads=threads, on_progress=on_progress) as tool:
        return tool.list_partitions()


def extract_partition(source, name, output=None, threads=DEFAULT_THREADS, on_progress=None):
    """提取指定分区。

    :param source: ZIP 文件 URL 或本地路径
    :param name: 分区名称（如 "boot"、"system"）
    :param output: 输出路径，默认 ``name.img``
    :param on_progress: 可选回调 ``(current, total, speed, elapsed)``
    :return: 成功 True；分区不存在抛 PartitionNotFoundError
    """
    with ZipPayloadTool(source, threads=threads, on_progress=on_progress) as tool:
        return tool.extract_partition(name, output)


def extract_file(source, name, output=None, threads=DEFAULT_THREADS, on_progress=None):
    """提取 ZIP 内任意文件。

    :param source: ZIP 文件 URL 或本地路径
    :param name: ZIP 内文件名（如 "payload.bin"、"META-INF/com/android/metadata"）
    :param output: 输出路径，默认取文件名
    :param on_progress: 可选回调 ``(current, total, speed, elapsed)``
    :return: 成功 True；文件不存在抛 FileNotFoundInZipError
    """
    with ZipPayloadTool(source, threads=threads, on_progress=on_progress) as tool:
        return tool.extract_file(name, output)


# =====================================================================
# 10. 命令行 CLI（仅当以脚本方式运行时生效，与库 API 完全隔离）
# =====================================================================

def _build_parser():
    parser = argparse.ArgumentParser(
        prog="remote-ota-dumper",
        description="从 Android OTA ZIP（payload.bin）中提取分区或文件，"
                    "支持本地路径与 HTTP(S) URL。",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="示例:\n"
               "  列出分区:\n"
               "    python remote_ota_dumper.py <zip-url-or-path>\n"
               "  提取分区:\n"
               "    python remote_ota_dumper.py <zip-url-or-path> boot\n"
               "    python remote_ota_dumper.py <zip-url-or-path> system -o system.img -t 16\n"
               "  提取文件:\n"
               "    python remote_ota_dumper.py <zip-url-or-path> payload.bin\n"
               "    python remote_ota_dumper.py <zip-url-or-path> META-INF/com/android/metadata\n"
               "    python remote_ota_dumper.py -f <zip-url-or-path> boot",
    )
    parser.add_argument("source", metavar="SOURCE",
                        help="ZIP 文件路径或 URL")
    parser.add_argument("name", metavar="NAME", nargs="?",
                        help="要提取的分区名称或文件名（省略则列出分区）")
    parser.add_argument("-l", "--list", action="store_true",
                        help="列出分区后退出")
    parser.add_argument("-o", "--output", metavar="PATH",
                        help="输出路径（默认：分区为 NAME.img，文件为 NAME）")
    parser.add_argument("-t", "--threads", metavar="N", type=int, default=DEFAULT_THREADS,
                        help=f"下载/解压线程数（默认 {DEFAULT_THREADS}）")
    parser.add_argument("--retries", metavar="N", type=int, default=MAX_RETRIES,
                        help=f"单次数据读取的重试次数（默认 {MAX_RETRIES}）")
    parser.add_argument("-f", "--force-file", action="store_true",
                        help="强制把 NAME 当作 ZIP 条目（文件）而非分区")
    parser.add_argument("-q", "--quiet", action="store_true",
                        help="不显示进度")
    parser.add_argument("--version", action="version",
                        version=f"%(prog)s {__version__}")
    return parser


def _make_progress_cb():
    def callback(current, total, speed, elapsed):
        ProgressUtils.print_progress(current, total, speed, elapsed)
    return callback


EXTRACT_RETRIES = 2  # 整次提取因网络失败时的自动重试次数


def _run_extraction(tool, mode, name, output):
    """执行一次提取；因网络失败（返回 False 且非用户中断）时自动重试。

    异常（文件/分区不存在等）不重试，原样抛出由调用方处理。
    """
    attempts = 0
    while True:
        if tool.stop_event.is_set():
            return False
        if mode == "file":
            ok = tool.extract_file(name, output)
        else:
            ok = tool.extract_partition(name, output)
        if ok or tool.stop_event.is_set():
            return ok
        attempts += 1
        if attempts > EXTRACT_RETRIES:
            return False
        print(f"\n提取失败（网络原因），{attempts * 3} 秒后自动重试 "
              f"{attempts}/{EXTRACT_RETRIES} ...")
        time.sleep(3 * attempts)


def _run_cli(argv=None):
    args = _build_parser().parse_args(argv)

    if not DataFetcher.is_remote(args.source) and not os.path.isfile(args.source):
        print(f"错误: 本地文件不存在 - {args.source}")
        return 1

    tool = ZipPayloadTool(args.source, threads=args.threads, max_retries=args.retries,
                          on_progress=None if args.quiet else _make_progress_cb())

    def signal_handler(sig, frame):
        print("\n接收到中断信号，正在终止操作...")
        tool.stop()
        threading.Timer(15.0, lambda: os._exit(1)).start()

    signal.signal(signal.SIGINT, signal_handler)

    try:
        tool.load()
    except Exception as e:
        print(f"错误: {e}")
        tool.close()
        return 1

    source_kind = "远程" if DataFetcher.is_remote(args.source) else "本地"
    print(f"源: {args.source} ({source_kind}, "
          f"{ProgressUtils.format_size(tool.file_size)})")
    if tool.metadata_warning:
        print(f"警告: {tool.metadata_warning}")

    try:
        if args.list or not args.name:
            partitions = tool.list_partitions()
            if not partitions:
                print("错误: 无法解析分区信息（未找到 payload.bin 或清单损坏）")
                return 1
            print("\n可用分区:")
            print(f"{'分区名称':<20} {'镜像大小':>12} {'下载大小':>14}")
            print("-" * 50)
            for p in partitions:
                print(f"{p['name']:<20} "
                      f"{ProgressUtils.format_size(p['image_size']):>12} "
                      f"{ProgressUtils.format_size(p['download_size']):>14}")
            return 0

        name = args.name
        file_out = args.output or os.path.basename(name.replace("\\", "/"))
        part_out = args.output or name + ".img"
        looks_like_file = "/" in name or "." in name

        if args.force_file:
            ok = _run_extraction(tool, "file", name, file_out)
            print()
            if ok:
                print(f"文件已保存至: {file_out}")
                return 0
            print("错误: 文件提取失败")
            return 1

        if looks_like_file and _run_extraction(tool, "file", name, file_out):
            print()
            print(f"文件已保存至: {file_out}")
            return 0
        if _run_extraction(tool, "partition", name, part_out):
            print()
            print(f"文件已保存至: {part_out}")
            return 0
        if not looks_like_file and _run_extraction(tool, "file", name, file_out):
            print()
            print(f"文件已保存至: {file_out}")
            return 0

        if tool.stop_event.is_set():
            print("操作已终止")
            return 1
        print(f"错误: 无法找到分区或文件 '{name}'")
        return 1
    except FileNotFoundInZipError as e:
        print(f"错误: {e}")
        return 1
    except PartitionNotFoundError as e:
        print(f"错误: {e}")
        return 1
    except DownloadInterrupted:
        print("\n操作被中断")
        return 1
    except KeyboardInterrupt:
        print("\n操作被用户中断")
        tool.stop()
        return 1
    except Exception as e:
        print(f"\n发生错误: {e}")
        return 1
    finally:
        tool.close()


def main():
    sys.exit(_run_cli())


if __name__ == "__main__":
    main()
