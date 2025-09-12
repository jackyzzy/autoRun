"""
日志工具模块
提供统一的日志配置和管理
"""

import logging
import logging.handlers
from pathlib import Path
from datetime import datetime
from typing import Optional
import os


class ColoredFormatter(logging.Formatter):
    """彩色日志格式化器"""
    
    COLORS = {
        'DEBUG': '\033[36m',     # 青色
        'INFO': '\033[32m',      # 绿色
        'WARNING': '\033[33m',   # 黄色
        'ERROR': '\033[31m',     # 红色
        'CRITICAL': '\033[35m',  # 紫色
    }
    RESET = '\033[0m'
    
    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{log_color}{record.levelname}{self.RESET}"
        return super().format(record)


def setup_logging(log_level: str = "INFO", 
                 log_file: Optional[str] = None,
                 log_dir: str = "logs",
                 enable_console: bool = True,
                 enable_file: bool = True) -> logging.Logger:
    """
    设置日志配置
    
    Args:
        log_level: 日志级别
        log_file: 日志文件名（不含路径）
        log_dir: 日志目录
        enable_console: 是否启用控制台输出
        enable_file: 是否启用文件输出
    """
    
    # 创建日志目录
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    # 获取根日志记录器
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    
    # 清除现有处理器
    logger.handlers.clear()
    
    # 日志格式
    console_format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    file_format = "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s"
    
    # 控制台处理器
    if enable_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
        
        # 使用彩色格式化器
        console_formatter = ColoredFormatter(console_format)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    # 文件处理器
    if enable_file:
        if not log_file:
            timestamp = datetime.now().strftime("%Y%m%d")
            log_file = f"playbook_{timestamp}.log"
        
        file_path = log_path / log_file
        
        # 使用RotatingFileHandler进行日志轮转
        file_handler = logging.handlers.RotatingFileHandler(
            file_path, 
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        
        file_formatter = logging.Formatter(file_format)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    # 设置第三方库日志级别
    logging.getLogger('paramiko').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """获取日志记录器"""
    return logging.getLogger(name)


class LogCapture:
    """日志捕获器，用于捕获特定时间段的日志"""
    
    def __init__(self, logger_name: str = None):
        self.logger_name = logger_name or ""
        self.captured_logs = []
        self.handler = None
        self.logger = None
        
    def start(self):
        """开始捕获日志"""
        self.logger = logging.getLogger(self.logger_name)
        self.handler = logging.Handler()
        self.handler.emit = lambda record: self.captured_logs.append(
            self.handler.format(record)
        )
        
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        self.handler.setFormatter(formatter)
        self.logger.addHandler(self.handler)
    
    def stop(self) -> list:
        """停止捕获日志并返回捕获的日志"""
        if self.handler and self.logger:
            self.logger.removeHandler(self.handler)
        
        return self.captured_logs.copy()
    
    def clear(self):
        """清除捕获的日志"""
        self.captured_logs.clear()
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


def setup_scenario_logger(scenario_name: str, log_dir: str = "logs") -> logging.Logger:
    """为特定场景设置专用日志记录器"""
    logger_name = f"scenario.{scenario_name}"
    logger = logging.getLogger(logger_name)
    
    # 如果已经设置过处理器，直接返回
    if logger.handlers:
        return logger
    
    # 创建场景专用日志文件
    log_path = Path(log_dir) / "scenarios"
    log_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_path / f"{scenario_name}_{timestamp}.log"
    
    # 文件处理器
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )
    file_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)
    
    # 不传播到根日志记录器，避免重复记录
    logger.propagate = True
    
    return logger