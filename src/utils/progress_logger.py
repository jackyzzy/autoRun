"""
进度日志工具
提供倒计时、进度条、状态指示器等通用日志功能
"""

import time
import threading
import logging
from typing import Optional, Callable, Any
from datetime import datetime


class CountdownLogger:
    """倒计时日志器"""

    def __init__(self, logger: logging.Logger, interval: int = 10):
        """
        Args:
            logger: 日志器实例
            interval: 倒计时输出间隔（秒）
        """
        self.logger = logger
        self.interval = interval
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start_countdown(self, total_seconds: int, message_template: str = "⏳ {message}: {remaining}s remaining..."):
        """
        开始倒计时

        Args:
            total_seconds: 总时间（秒）
            message_template: 消息模板，支持{message}和{remaining}占位符
        """
        if self._thread and self._thread.is_alive():
            self.stop_countdown()

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._countdown_worker,
            args=(total_seconds, message_template),
            daemon=True
        )
        self._thread.start()

    def stop_countdown(self):
        """停止倒计时"""
        if self._stop_event:
            self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)

    def _countdown_worker(self, total_seconds: int, message_template: str):
        """倒计时工作线程"""
        start_time = time.time()
        next_log_time = start_time + self.interval

        while not self._stop_event.is_set():
            current_time = time.time()
            elapsed = current_time - start_time
            remaining = max(0, total_seconds - elapsed)

            # 检查是否到了输出时间
            if current_time >= next_log_time or remaining <= 0:
                if remaining > 0:
                    self.logger.info(message_template.format(
                        message="Countdown",
                        remaining=int(remaining)
                    ))
                    next_log_time = current_time + self.interval
                else:
                    # 倒计时结束
                    break

            # 短暂休眠
            time.sleep(1)


class ProgressIndicator:
    """进度指示器"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self._start_time: Optional[datetime] = None

    def start(self, message: str = "Starting..."):
        """开始进度指示"""
        self._start_time = datetime.now()
        self.logger.info(f"🚀 {message}")

    def update(self, current: int, total: int, message: str = ""):
        """更新进度"""
        if total <= 0:
            return

        percentage = (current / total) * 100
        elapsed = self._get_elapsed_seconds()

        progress_bar = self._create_progress_bar(current, total)

        if message:
            self.logger.info(f"📊 [{progress_bar}] {percentage:.1f}% - {message} (elapsed: {elapsed:.1f}s)")
        else:
            self.logger.info(f"📊 [{progress_bar}] {percentage:.1f}% (elapsed: {elapsed:.1f}s)")

    def complete(self, message: str = "Completed"):
        """完成进度"""
        elapsed = self._get_elapsed_seconds()
        self.logger.info(f"✅ {message} (total time: {elapsed:.1f}s)")

    def _get_elapsed_seconds(self) -> float:
        """获取已用时间"""
        if self._start_time:
            return (datetime.now() - self._start_time).total_seconds()
        return 0.0

    def _create_progress_bar(self, current: int, total: int, width: int = 20) -> str:
        """创建进度条"""
        if total <= 0:
            return "=" * width

        filled = int((current / total) * width)
        bar = "=" * filled + "-" * (width - filled)
        return f"{current}/{total} {bar}"


class StatusSpinner:
    """状态旋转指示器"""

    SPINNER_CHARS = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

    def __init__(self, logger: logging.Logger, interval: float = 0.5):
        """
        Args:
            logger: 日志器实例
            interval: 旋转间隔（秒）
        """
        self.logger = logger
        self.interval = interval
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._current_message = ""

    def start(self, message: str = "Processing..."):
        """开始旋转指示"""
        if self._thread and self._thread.is_alive():
            self.stop()

        self._current_message = message
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._spinner_worker,
            daemon=True
        )
        self._thread.start()

    def update_message(self, message: str):
        """更新显示消息"""
        self._current_message = message

    def stop(self, final_message: str = ""):
        """停止旋转指示"""
        if self._stop_event:
            self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)

        if final_message:
            self.logger.info(final_message)

    def _spinner_worker(self):
        """旋转指示器工作线程"""
        char_index = 0

        while not self._stop_event.is_set():
            char = self.SPINNER_CHARS[char_index % len(self.SPINNER_CHARS)]
            self.logger.info(f"{char} {self._current_message}")

            char_index += 1
            time.sleep(self.interval)


class EnhancedProgressLogger:
    """增强的进度日志器 - 组合多种进度显示功能"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.countdown = CountdownLogger(logger)
        self.progress = ProgressIndicator(logger)
        self.spinner = StatusSpinner(logger)

    def log_waiting_with_countdown(self, total_seconds: int, message: str, interval: int = 10):
        """
        记录等待过程并显示倒计时

        Args:
            total_seconds: 等待总时间
            message: 等待描述
            interval: 倒计时间隔
        """
        self.logger.info(f"⏳ {message} ({total_seconds}s)")

        # 创建自定义消息模板
        template = f"⏳ {message}: {{remaining}}s remaining..."

        self.countdown.start_countdown(total_seconds, template)
        time.sleep(total_seconds)
        self.countdown.stop_countdown()

        self.logger.info(f"✅ {message} completed")

    def log_retry_progress(self, current_attempt: int, max_attempts: int, operation: str, delay: int = 0):
        """
        记录重试进度

        Args:
            current_attempt: 当前尝试次数（从1开始）
            max_attempts: 最大尝试次数
            operation: 操作描述
            delay: 重试延迟时间
        """
        progress_bar = self.progress._create_progress_bar(current_attempt, max_attempts)
        self.logger.info(f"🔄 [{progress_bar}] {operation} (attempt {current_attempt}/{max_attempts})")

        if delay > 0 and current_attempt < max_attempts:
            self.log_waiting_with_countdown(delay, f"Retrying {operation} in", interval=5)

    def cleanup(self):
        """清理所有进度指示器"""
        self.countdown.stop_countdown()
        self.spinner.stop()


# 便捷函数
def create_progress_logger(logger_name: str) -> EnhancedProgressLogger:
    """创建增强进度日志器的便捷函数"""
    logger = logging.getLogger(logger_name)
    return EnhancedProgressLogger(logger)


def log_countdown(logger: logging.Logger, seconds: int, message: str, interval: int = 10):
    """简单的倒计时日志函数"""
    countdown_logger = CountdownLogger(logger, interval)
    countdown_logger.start_countdown(seconds, f"⏳ {message}: {{remaining}}s remaining...")
    time.sleep(seconds)
    countdown_logger.stop_countdown()