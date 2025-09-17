"""日志工具模块

提供统一的日志配置和管理，支持结构化日志、多种输出格式、
分级日志管理等功能。

主要功能:
    - 彩色控制台日志输出
    - 文件日志轮转和归档
    - 结构化日志支持
    - 场景专用日志记录
    - 日志捕获和过滤
    - 性能监控集成

典型用法:
    >>> logger = setup_logging("INFO", enable_structured=True)
    >>> logger.info("System started", extra={"component": "core", "version": "1.0"})
"""

import logging
import logging.handlers
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, Union
import json
import os
import sys
from dataclasses import dataclass, asdict
from enum import Enum


class LogLevel(Enum):
    """日志级别枚举"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class LogEntry:
    """结构化日志条目"""
    timestamp: str
    level: str
    logger_name: str
    message: str
    component: Optional[str] = None
    operation: Optional[str] = None
    node_name: Optional[str] = None
    scenario: Optional[str] = None
    duration: Optional[float] = None
    error_code: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        result = asdict(self)
        # 移除None值
        return {k: v for k, v in result.items() if v is not None}

    def to_json(self) -> str:
        """转换为JSON格式"""
        return json.dumps(self.to_dict(), ensure_ascii=False, default=str)


class StructuredFormatter(logging.Formatter):
    """结构化日志格式化器"""

    def format(self, record) -> str:
        """格式化日志记录为JSON"""
        # 提取额外字段
        extra_fields = {}
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname',
                          'filename', 'module', 'lineno', 'funcName', 'created',
                          'msecs', 'relativeCreated', 'thread', 'threadName',
                          'processName', 'process', 'getMessage', 'exc_info',
                          'exc_text', 'stack_info', 'message']:
                extra_fields[key] = value

        log_entry = LogEntry(
            timestamp=datetime.fromtimestamp(record.created).isoformat(),
            level=record.levelname,
            logger_name=record.name,
            message=record.getMessage(),
            component=extra_fields.get('component'),
            operation=extra_fields.get('operation'),
            node_name=extra_fields.get('node_name'),
            scenario=extra_fields.get('scenario'),
            duration=extra_fields.get('duration'),
            error_code=extra_fields.get('error_code'),
            metadata=extra_fields.get('metadata', extra_fields if extra_fields else None)
        )

        return log_entry.to_json()


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
                 enable_file: bool = True,
                 enable_structured: bool = False,
                 console_format: str = "human",
                 file_format: str = "detailed") -> logging.Logger:
    """设置日志配置

    Args:
        log_level: 日志级别 (DEBUG/INFO/WARNING/ERROR/CRITICAL)
        log_file: 日志文件名（不含路径），None时自动生成
        log_dir: 日志目录路径
        enable_console: 是否启用控制台输出
        enable_file: 是否启用文件输出
        enable_structured: 是否启用结构化日志（JSON格式）
        console_format: 控制台输出格式 (human/json/compact)
        file_format: 文件输出格式 (detailed/json/compact)

    Returns:
        配置好的根日志记录器

    Examples:
        >>> # 基础配置
        >>> logger = setup_logging("INFO")
        >>>
        >>> # 结构化日志配置
        >>> logger = setup_logging("DEBUG", enable_structured=True, console_format="json")
        >>>
        >>> # 仅文件日志
        >>> logger = setup_logging("INFO", enable_console=False, log_file="app.log")
    """
    
    # 创建日志目录
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    # 获取根日志记录器
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    
    # 清除现有处理器
    logger.handlers.clear()
    
    # 定义不同的日志格式
    formats = {
        "human": "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        "compact": "%(levelname)s | %(name)s | %(message)s",
        "detailed": "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s",
        "json": None  # 将使用StructuredFormatter
    }

    # 控制台处理器
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))

        # 选择格式化器
        if enable_structured or console_format == "json":
            console_formatter = StructuredFormatter()
        else:
            format_string = formats.get(console_format, formats["human"])
            console_formatter = ColoredFormatter(format_string)

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

        # 选择文件格式化器
        if enable_structured or file_format == "json":
            file_formatter = StructuredFormatter()
        else:
            format_string = formats.get(file_format, formats["detailed"])
            file_formatter = logging.Formatter(format_string)

        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    # 设置第三方库日志级别
    logging.getLogger('paramiko').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """获取日志记录器"""
    return logging.getLogger(name)


class StructuredLogger:
    """结构化日志记录器包装类

    提供便捷的结构化日志记录方法，自动添加上下文信息。

    Examples:
        >>> slogger = StructuredLogger("my.module", component="core")
        >>> slogger.info("Operation started", operation="test", duration=1.5)
        >>> slogger.error("Operation failed", error_code="TEST001", node_name="node1")
    """

    def __init__(self, logger_name: str, **default_context):
        self.logger = logging.getLogger(logger_name)
        self.default_context = default_context

    def _log(self, level: int, message: str, **context):
        """内部日志方法"""
        # 合并默认上下文和当前上下文
        full_context = {**self.default_context, **context}
        self.logger.log(level, message, extra=full_context)

    def debug(self, message: str, **context):
        """Debug级别日志"""
        self._log(logging.DEBUG, message, **context)

    def info(self, message: str, **context):
        """Info级别日志"""
        self._log(logging.INFO, message, **context)

    def warning(self, message: str, **context):
        """Warning级别日志"""
        self._log(logging.WARNING, message, **context)

    def error(self, message: str, **context):
        """Error级别日志"""
        self._log(logging.ERROR, message, **context)

    def critical(self, message: str, **context):
        """Critical级别日志"""
        self._log(logging.CRITICAL, message, **context)

    def operation_start(self, operation: str, **context):
        """记录操作开始"""
        self.info(f"Operation '{operation}' started", operation=operation, **context)

    def operation_complete(self, operation: str, duration: float, **context):
        """记录操作完成"""
        self.info(f"Operation '{operation}' completed",
                 operation=operation, duration=duration, **context)

    def operation_failed(self, operation: str, error: str, **context):
        """记录操作失败"""
        self.error(f"Operation '{operation}' failed: {error}",
                  operation=operation, error=error, **context)


def get_structured_logger(name: str, **default_context) -> StructuredLogger:
    """获取结构化日志记录器

    Args:
        name: 日志记录器名称
        **default_context: 默认上下文字段

    Returns:
        结构化日志记录器实例

    Examples:
        >>> slogger = get_structured_logger("node.manager", component="node_mgr")
        >>> slogger.info("Node connected", node_name="node1", host="192.168.1.1")
    """
    return StructuredLogger(name, **default_context)


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