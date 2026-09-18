#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GUI.pyw —— Remote OTA Dumper 轻量图形界面（tkinter，仅标准库）。

用法: 双击 GUI.pyw（或 pythonw GUI.pyw）运行，python GUI.pyw 可用于调试。

功能:
  * 输入 ZIP 链接或选择本地文件，获取并列出 OTA 信息（设备/Android 版本/安全补丁/时间戳）
    与全部分区（镜像大小 / 下载大小）
  * 勾选多个分区一键下载（顺序执行），进度条与「已下载 / 总大小」直接显示在分区列表里
  * 下载成功自动取消勾选；已存在的完整分区自动跳过；取消后不残留半成品文件
  * 「日志」按钮弹出查看运行日志
"""

import json
import os
import queue
import sys
import threading
import tkinter as tk
from datetime import datetime
from tkinter import filedialog, messagebox, ttk

import remote_ota_dumper as zpe


def _default_download_dir():
    """默认下载目录：exe/脚本所在目录下的 downloads 子目录。

    Nuitka/PyInstaller 单文件模式下 __file__ 指向 %TEMP% 里的解压临时目录，
    必须用 sys.argv[0] 定位程序真实位置（源码运行时它同样指向 GUI.pyw）。
    目录不可写时依次回退当前目录、用户 Downloads。
    """
    candidates = []
    try:
        candidates.append(os.path.dirname(os.path.abspath(sys.argv[0])))
    except Exception:
        pass
    candidates.append(os.getcwd())
    candidates.append(os.path.join(os.path.expanduser("~"), "Downloads"))
    for base in candidates:
        try:
            d = os.path.join(base, "downloads")
            os.makedirs(d, exist_ok=True)
            return d
        except OSError:
            continue
    return os.path.join(os.path.expanduser("~"), "Downloads")


DEFAULT_DIR = _default_download_dir()

# API 级别 -> Android 版本号
ANDROID_VER = {33: "13", 34: "14", 35: "15", 36: "16", 37: "17"}

STATUS_WAIT = "等待中"
STATUS_DOWNLOADING = "下载中..."
STATUS_OK = "完成 ✓"
STATUS_FAIL = "失败"
STATUS_SKIP = "已存在，跳过"


class ScrollFrame(ttk.Frame):
    """带垂直滚动条的容器，内容放入 self.inner"""

    def __init__(self, master, height=200):
        super().__init__(master)
        self.canvas = tk.Canvas(self, highlightthickness=0, height=height)
        self.vsb = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.inner = ttk.Frame(self.canvas)
        self._win = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")
        self.inner.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")),
        )
        self.canvas.bind(
            "<Configure>",
            lambda e: self.canvas.itemconfigure(self._win, width=e.width),
        )
        self.canvas.configure(yscrollcommand=self.vsb.set)
        self.canvas.pack(side="left", fill="both", expand=True)
        self.vsb.pack(side="right", fill="y")
        self.canvas.bind("<Enter>", self._bind_wheel)
        self.canvas.bind("<Leave>", self._unbind_wheel)

    def _bind_wheel(self, _e):
        self.canvas.bind_all("<MouseWheel>", self._on_wheel)

    def _unbind_wheel(self, _e):
        self.canvas.unbind_all("<MouseWheel>")

    def _on_wheel(self, e):
        self.canvas.yview_scroll(int(-e.delta / 120), "units")


def format_ota_info(info):
    """把 OTA 元数据字典格式化为一行摘要；无数据时返回提示。"""
    if not info:
        return "未找到 META-INF/com/android/metadata（部分包无此文件）"
    items = []

    device = info.get("pre-device", "")
    if device:
        vendor = info.get("post-build", "").split("/", 1)[0] if "/" in info.get("post-build", "") else ""
        items.append(f"设备: {device}" + (f" ({vendor})" if vendor else ""))

    sdk = info.get("post-sdk-level", "")
    if sdk.isdigit():
        ver = ANDROID_VER.get(int(sdk), "?")
        items.append(f"系统: Android {ver} (API {sdk})")

    if info.get("post-build-incremental"):
        items.append(f"版本: {info['post-build-incremental']}")

    if info.get("post-security-patch-level"):
        items.append(f"安全补丁: {info['post-security-patch-level']}")

    ts = info.get("post-timestamp", "")
    if ts.isdigit():
        try:
            items.append(f"构建时间: {datetime.fromtimestamp(int(ts)):%Y-%m-%d %H:%M}")
        except (OSError, ValueError, OverflowError):
            pass

    if info.get("ota-type"):
        items.append(f"OTA 类型: {info['ota-type']}")

    return " | ".join(items)


class ZipGUI:
    def __init__(self, root):
        self.root = root
        root.title("Remote OTA Dumper - OTA 分区提取")
        root.geometry("800x680")
        root.minsize(720, 560)

        self.source_var = tk.StringVar()
        self.threads_var = tk.StringVar(value="8")
        self.save_dir_var = tk.StringVar(value=DEFAULT_DIR)
        self.status_var = tk.StringVar(value="输入链接或选择本地文件后，点击「获取分区」")
        self.summary_var = tk.StringVar(value="未选择分区")
        self.ota_info_var = tk.StringVar(value="")

        self.tool = None            # ZipPayloadTool 实例（获取分区后持有，下载复用缓存）
        self.partitions = []        # [{name, image_size, download_size}, ...]
        self.check_vars = {}        # 分区名 -> BooleanVar
        self.progress_widgets = {}  # 分区名 -> (Progressbar, 状态 StringVar)
        self.events = queue.Queue()
        self.download_thread = None
        self._busy = False

        # 下载队列：支持下载进行中追加新勾选的分区
        self._pending = []          # 待下载分区名（FIFO）
        self._pending_lock = threading.Lock()
        self._new_item = threading.Event()

        self.log_lines = []         # 日志内容（「日志」按钮弹窗查看）
        self.log_window = None

        self._build_widgets()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(100, self._poll_events)

    # ================= 界面搭建 =================

    def _build_widgets(self):
        pad = {"padx": 6, "pady": 3}

        # 源：链接或本地文件（获取分区按钮就在旁边，流程一目了然）
        frm = ttk.LabelFrame(self.root, text="源（URL 或本地路径）")
        frm.pack(fill="x", **pad)
        ttk.Entry(frm, textvariable=self.source_var).pack(
            side="left", fill="x", expand=True, padx=4, pady=4)
        ttk.Button(frm, text="选择本地文件...", command=self._browse).pack(
            side="left", padx=(0, 4))
        ttk.Button(frm, text="获取分区", command=self._fetch_partitions).pack(
            side="left", padx=(0, 4))
        for child in frm.winfo_children():
            if isinstance(child, ttk.Entry):
                child.bind("<Return>", lambda _e: self._fetch_partitions())

        # 选项
        frm2 = ttk.LabelFrame(self.root, text="选项")
        frm2.pack(fill="x", **pad)
        ttk.Label(frm2, text="线程数").pack(side="left", padx=(4, 2))
        ttk.Spinbox(frm2, from_=1, to=64, textvariable=self.threads_var,
                    width=5).pack(side="left")
        ttk.Label(frm2, text="保存到").pack(side="left", padx=(14, 2))
        ttk.Entry(frm2, textvariable=self.save_dir_var).pack(
            side="left", fill="x", expand=True, padx=2)
        ttk.Button(frm2, text="选择目录...", command=self._browse_dir).pack(side="left", padx=4)
        ttk.Button(frm2, text="全选", command=lambda: self._select_all(True)).pack(
            side="left", padx=(14, 2))
        ttk.Button(frm2, text="全不选", command=lambda: self._select_all(False)).pack(
            side="left", padx=(0, 4))

        # OTA 信息
        self.ota_frame = ttk.LabelFrame(self.root, text="OTA 信息")
        self.ota_frame.pack(fill="x", **pad)
        ttk.Label(self.ota_frame, textvariable=self.ota_info_var, wraplength=760,
                  justify="left", anchor="w").pack(fill="x", padx=6, pady=4)

        # 分区列表（含勾选、大小、进度条、状态；进度直接融合在表里）
        self.part_frame = ScrollFrame(self.root, height=300)
        self.part_frame.pack(fill="both", expand=True, **pad)

        # 汇总 + 操作
        frm3 = ttk.Frame(self.root)
        frm3.pack(fill="x", **pad)
        ttk.Label(frm3, textvariable=self.summary_var).pack(side="left")
        ttk.Button(frm3, text="日志", command=self._open_log).pack(side="right", padx=4)
        ttk.Button(frm3, text="取消", command=self._cancel).pack(side="right", padx=4)
        ttk.Button(frm3, text="开始下载", command=self._start_download).pack(side="right")

        # 状态栏
        ttk.Label(self.root, textvariable=self.status_var, relief="sunken").pack(
            fill="x", side="bottom")

    # ================= 日志 =================

    def _log(self, msg):
        self.log_lines.append(f"[{datetime.now():%H:%M:%S}] {msg}")
        if len(self.log_lines) > 500:
            self.log_lines = self.log_lines[-500:]
        if self.log_window is not None and self.log_window.winfo_exists():
            text = self.log_window.text
            text.config(state="normal")
            text.insert("end", f"[{datetime.now():%H:%M:%S}] {msg}\n")
            text.see("end")
            text.config(state="disabled")

    def _open_log(self):
        if self.log_window is not None and self.log_window.winfo_exists():
            self.log_window.lift()
            return
        win = tk.Toplevel(self.root)
        win.title("日志")
        win.geometry("560x320")
        text = tk.Text(win, state="disabled")
        sb = ttk.Scrollbar(win, orient="vertical", command=text.yview)
        text.configure(yscrollcommand=sb.set)
        text.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")
        text.config(state="normal")
        text.insert("end", "\n".join(self.log_lines) + "\n")
        text.config(state="disabled")
        self.log_window = win
        self.log_window.text = text
        win.protocol("WM_DELETE_WINDOW", lambda: win.destroy())

    # ================= 交互 =================

    def _browse(self):
        path = filedialog.askopenfilename(
            title="选择 ZIP 文件",
            filetypes=[("ZIP 文件", "*.zip"), ("所有文件", "*.*")])
        if path:
            self.source_var.set(path)

    def _browse_dir(self):
        path = filedialog.askdirectory(title="选择保存目录")
        if path:
            self.save_dir_var.set(path)

    def _select_all(self, value):
        for var in self.check_vars.values():
            var.set(value)
        self._update_summary()

    def _update_summary(self):
        selected = [n for n, v in self.check_vars.items() if v.get()]
        total = sum(p["download_size"] for p in self.partitions if p["name"] in selected)
        self.summary_var.set(
            f"已选 {len(selected)} 个分区，下载总量 {zpe.ProgressUtils.format_size(total)}")

    def _set_busy(self, busy, status=None):
        self._busy = busy
        if status is not None:
            self.status_var.set(status)

    # ================= 获取分区 =================

    def _fetch_partitions(self):
        if self._busy:
            return
        source = self.source_var.get().strip()
        if not source:
            messagebox.showwarning("提示", "请先输入 ZIP 链接或选择本地文件")
            return
        try:
            threads = max(1, int(self.threads_var.get()))
        except ValueError:
            threads = 8
        self._set_busy(True, "正在获取分区信息...")
        threading.Thread(target=self._fetch_worker,
                         args=(source, threads), daemon=True).start()

    def _fetch_worker(self, source, threads):
        try:
            if self.tool is not None:
                self.tool.close()
            self.tool = zpe.ZipPayloadTool(source, threads=threads)
            self.tool.load()
            parts = self.tool.list_partitions()
            if parts is None:
                self.events.put(("parts_error", "未找到 payload.bin 或清单解析失败"))
            else:
                ota_info = self.tool.get_ota_info()
                self.events.put(("parts", parts, ota_info))
        except Exception as e:
            self.events.put(("parts_error", str(e)))

    def _render_partitions(self):
        inner = self.part_frame.inner
        for w in inner.winfo_children():
            w.destroy()
        self.check_vars.clear()
        self.progress_widgets.clear()

        # 表头 + 分隔线（所有行共享同一网格，各列严格对齐）
        ttk.Label(inner, text="选择", font=("", 9, "bold")).grid(
            row=0, column=0, sticky="w", padx=4)
        ttk.Label(inner, text="分区名称", font=("", 9, "bold")).grid(
            row=0, column=1, sticky="w", padx=4)
        ttk.Label(inner, text="镜像大小", font=("", 9, "bold")).grid(
            row=0, column=2, sticky="e", padx=6)
        ttk.Label(inner, text="下载大小", font=("", 9, "bold")).grid(
            row=0, column=3, sticky="e", padx=6)
        ttk.Label(inner, text="进度", font=("", 9, "bold")).grid(
            row=0, column=4, sticky="w", padx=6)
        ttk.Label(inner, text="状态", font=("", 9, "bold")).grid(
            row=0, column=5, sticky="e", padx=6)
        ttk.Separator(inner, orient="horizontal").grid(
            row=1, column=0, columnspan=6, sticky="ew", pady=2)
        inner.columnconfigure(1, weight=1)
        inner.columnconfigure(4, weight=1)

        for i, p in enumerate(self.partitions, start=2):
            var = tk.BooleanVar(value=False)
            self.check_vars[p["name"]] = var
            ttk.Checkbutton(inner, variable=var,
                            command=self._update_summary).grid(
                row=i, column=0, sticky="w", padx=4, pady=1)
            ttk.Label(inner, text=p["name"], anchor="w").grid(
                row=i, column=1, sticky="w", padx=4)
            ttk.Label(inner, text=zpe.ProgressUtils.format_size(p["image_size"]),
                      anchor="e").grid(row=i, column=2, sticky="e", padx=6)
            ttk.Label(inner, text=zpe.ProgressUtils.format_size(p["download_size"]),
                      anchor="e").grid(row=i, column=3, sticky="e", padx=6)
            bar = ttk.Progressbar(inner, maximum=1000)
            bar.grid(row=i, column=4, sticky="ew", padx=6, pady=1)
            status_var = tk.StringVar(value=STATUS_WAIT)
            ttk.Label(inner, textvariable=status_var, width=24, anchor="e").grid(
                row=i, column=5, sticky="e", padx=6)
            self.progress_widgets[p["name"]] = (bar, status_var)
        self._update_summary()

    # ================= 下载 =================

    def _start_download(self):
        if self.tool is None or not self.partitions:
            messagebox.showwarning("提示", "请先获取分区信息")
            return
        selected = [n for n, v in self.check_vars.items() if v.get()]
        if not selected:
            messagebox.showwarning("提示", "请先勾选要下载的分区")
            return
        save_dir = self.save_dir_var.get().strip() or DEFAULT_DIR
        try:
            os.makedirs(save_dir, exist_ok=True)
        except OSError as e:
            messagebox.showerror("错误", f"无法创建保存目录: {e}")
            return
        self.save_dir_var.set(save_dir)

        # 新勾选的分区先进待处理队列
        with self._pending_lock:
            self._pending.extend(selected)
        self._new_item.set()

        if self._busy and self.download_thread is not None \
                and self.download_thread.is_alive():
            # 下载进行中：追加到当前队列，不打断进行中的任务
            self.status_var.set(f"已加入队列 {len(selected)} 个分区")
            self._log(f"已加入队列: {', '.join(selected)}")
            return

        # 新起下载队列
        self._new_item.clear()
        self._set_busy(True, "下载中...")
        self.download_thread = threading.Thread(
            target=self._download_worker, args=(save_dir,), daemon=True)
        self.download_thread.start()

    def _pop_pending(self):
        with self._pending_lock:
            if self._pending:
                return self._pending.pop(0)
        return None

    def _download_worker(self, save_dir):
        try:
            while True:
                if self.tool.stop_event.is_set():
                    break
                name = self._pop_pending()
                if name is None:
                    # 队列空：短暂等待可能的新增任务（下载中追加的分区会被接住）
                    if self._new_item.wait(1.0):
                        self._new_item.clear()
                        continue
                    break
                self.events.put(("download_start", name))
                out = os.path.join(save_dir, name + ".img")
                # 跳过条件：文件存在 + 大小一致 + 包指纹一致（换链接/换包后
                # 即使分区大小相同也不会误判为已下载）
                expected = next(
                    (p["image_size"] for p in self.partitions if p["name"] == name),
                    None)
                if self._is_downloaded(out, expected):
                    self.events.put(("download_skip", name))
                    continue

                # 先下载到 .part 临时文件：成功才改名，失败/取消即删除，
                # 保证正式目录里不会残留半成品（半成品大小与完整镜像相同，
                # 不这样做会被“已存在”误判跳过）
                tmp = out + ".part"

                def on_progress(current, total, _speed, _elapsed, n=name):
                    self.events.put(("progress", n, current, total))

                self.tool.on_progress = on_progress
                try:
                    ok = self.tool.extract_partition(name, tmp)
                    if ok:
                        os.replace(tmp, out)
                        self._write_meta(out, name)
                    else:
                        self._remove_file(tmp)
                    self.events.put(("download_done", name, ok))
                except Exception as e:
                    self._remove_file(tmp)
                    self.events.put(("download_error", name, str(e)))
                    self.tool.stop()
                    break
        finally:
            self.events.put(("all_done",))

    @staticmethod
    def _remove_file(path):
        try:
            os.remove(path)
        except OSError:
            pass

    def _is_downloaded(self, out, expected):
        """是否已有属于当前包的完整文件：文件存在 + 大小一致 + 包指纹一致。"""
        if expected is None or not os.path.isfile(out) \
                or os.path.getsize(out) != expected:
            return False
        package_id = self.tool.package_id if self.tool is not None else None
        if not package_id:
            return False  # 拿不到包指纹（未解析成功）→ 不跳过，保守重新下载
        try:
            with open(out + ".meta", "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception:
            return False  # 无旁车标记（旧版本产物/手动拷贝）→ 不跳过
        return meta.get("package_id") == package_id

    def _write_meta(self, out, name):
        """下载成功后写入旁车标记（记录所属包的指纹）"""
        try:
            with open(out + ".meta", "w", encoding="utf-8") as f:
                json.dump({
                    "name": name,
                    "package_id": self.tool.package_id,
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }, f, ensure_ascii=False)
        except OSError:
            pass

    def _cancel(self):
        if self._busy and self.download_thread is not None \
                and self.download_thread.is_alive():
            # 有进行中的下载：优雅停止，队列结束后自动复位
            if self.tool is not None:
                self.tool.stop()
            self.status_var.set("正在取消...")
            self._log("正在取消...")
        else:
            # 空闲时点取消：不改任何状态，否则会卡在“正在取消”且 stop 标志
            # 残留导致下一次下载瞬间结束
            self.status_var.set("当前没有进行中的下载")
            self._log("点击取消：当前没有进行中的下载")

    def _on_close(self):
        if self.tool is not None:
            self.tool.stop()
            self.tool.close()
        self.root.destroy()

    # ================= 事件泵 =================

    def _poll_events(self):
        try:
            while True:
                self._handle_event(self.events.get_nowait())
        except queue.Empty:
            pass
        self.root.after(100, self._poll_events)

    def _handle_event(self, event):
        kind = event[0]
        if kind == "parts":
            self._set_busy(False)  # 先解除忙碌，避免渲染间隙挡住后续操作
            self.partitions = event[1]
            ota_info = event[2] if len(event) > 2 else None
            self.ota_info_var.set(format_ota_info(ota_info))
            self._render_partitions()
            self.status_var.set(f"共 {len(self.partitions)} 个分区，请勾选后下载")
            self._log(f"获取成功：{len(self.partitions)} 个分区")
        elif kind == "parts_error":
            self.status_var.set("获取分区失败")
            self._log(f"错误: {event[1]}")
            messagebox.showerror("错误", event[1])
            self._set_busy(False)
        elif kind == "download_start":
            name = event[1]
            bar, status_var = self.progress_widgets.get(name, (None, None))
            if bar is not None:
                bar["value"] = 0
                status_var.set(STATUS_DOWNLOADING)
        elif kind == "progress":
            name, current, total = event[1], event[2], event[3]
            bar, status_var = self.progress_widgets.get(name, (None, None))
            if bar is not None:
                bar["value"] = (current / total * 1000) if total else 0
                status_var.set(
                    f"{zpe.ProgressUtils.format_size(current)} / "
                    f"{zpe.ProgressUtils.format_size(total)}")
        elif kind == "download_skip":
            name = event[1]
            bar, status_var = self.progress_widgets.get(name, (None, None))
            if bar is not None:
                bar["value"] = 1000
                status_var.set(STATUS_SKIP)
            self.status_var.set(f"{name} 已存在（同包），跳过")
            self._log(f"{name} 已存在且属于当前包，跳过下载")
        elif kind == "download_done":
            name, ok = event[1], event[2]
            bar, status_var = self.progress_widgets.get(name, (None, None))
            if bar is not None:
                status_var.set(STATUS_OK if ok else STATUS_FAIL)
            self.status_var.set(f"{name} 下载{'完成' if ok else '失败'}")
            self._log(f"{name} 下载{'完成' if ok else '失败'}")
            if ok and name in self.check_vars:
                # 下载成功自动取消勾选，避免下次误带旧分区
                self.check_vars[name].set(False)
                self._update_summary()
        elif kind == "download_error":
            name, msg = event[1], event[2]
            bar, status_var = self.progress_widgets.get(name, (None, None))
            if bar is not None:
                status_var.set(STATUS_FAIL)
            self.status_var.set(f"{name} 错误: {msg[:60]}")
            self._log(f"{name} 错误: {msg}")
        elif kind == "all_done":
            if self.tool is not None:
                # 取消后复位停止标志，允许后续重新选择下载
                self.tool.stop_event.clear()
            self.status_var.set("下载队列结束")
            self._log("下载队列结束")
            self._set_busy(False)


def main():
    root = tk.Tk()
    ZipGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
