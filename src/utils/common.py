"""通用工具函数

提供项目中常用的公共功能，减少代码重复。

主要功能:
    - 统一的错误处理和日志记录
    - 配置文件处理辅助函数
    - 重试机制装饰器
    - 路径和文件操作辅助函数
    - 时间和格式化工具
"""

import logging
import time
import functools
from typing import Dict, Any, Optional, Callable, TypeVar, Union, List
from pathlib import Path
import yaml
import json
from datetime import datetime
import os

from ..playbook.exceptions import PlaybookException, ConfigurationError, wrap_exception

# 类型变量
T = TypeVar('T')
F = TypeVar('F', bound=Callable[..., Any])


def setup_module_logger(module_name: str, level: str = "INFO") -> logging.Logger:
    """为模块设置标准化的日志记录器

    Args:
        module_name: 模块名称
        level: 日志级别

    Returns:
        配置好的日志记录器

    Examples:
        >>> logger = setup_module_logger("my_module")
        >>> logger.info("This is a log message")
    """
    logger = logging.getLogger(module_name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper()))
    return logger


def safe_execute(func: Callable[..., T],
                *args,
                **kwargs) -> Dict[str, Any]:
    """安全执行函数，统一异常处理

    Args:
        func: 要执行的函数
        *args: 函数位置参数
        **kwargs: 函数关键字参数

    Returns:
        包含执行结果的字典，格式:
        {
            'success': bool,
            'result': Any,  # 仅在成功时存在
            'error': str,   # 仅在失败时存在
            'exception': Exception  # 仅在失败时存在
        }

    Examples:
        >>> result = safe_execute(risky_function, arg1, kwarg1="value")
        >>> if result['success']:
        ...     print(f"Result: {result['result']}")
        ... else:
        ...     print(f"Error: {result['error']}")
    """
    try:
        result = func(*args, **kwargs)
        return {
            'success': True,
            'result': result
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'exception': e
        }


def retry_on_failure(max_attempts: int = 3,
                    delay: float = 1.0,
                    backoff_factor: float = 2.0,
                    exceptions: tuple = (Exception,),
                    logger: Optional[logging.Logger] = None) -> Callable[[F], F]:
    """重试装饰器

    Args:
        max_attempts: 最大重试次数
        delay: 初始延迟时间（秒）
        backoff_factor: 延迟递增因子
        exceptions: 需要重试的异常类型
        logger: 日志记录器

    Returns:
        装饰器函数

    Examples:
        >>> @retry_on_failure(max_attempts=3, delay=0.5)
        ... def unreliable_function():
        ...     # 可能失败的操作
        ...     pass
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None

            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if logger:
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_attempts} failed for {func.__name__}: {e}"
                        )

                    if attempt < max_attempts - 1:
                        time.sleep(current_delay)
                        current_delay *= backoff_factor
                    else:
                        if logger:
                            logger.error(f"All {max_attempts} attempts failed for {func.__name__}")
                        raise last_exception

            # 这里永远不会执行，但为了类型检查
            raise last_exception

        return wrapper
    return decorator


def load_yaml_config(file_path: Union[str, Path],
                    required: bool = True,
                    default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """加载YAML配置文件

    Args:
        file_path: 配置文件路径
        required: 是否必需存在
        default: 默认配置（当文件不存在且不是必需时使用）

    Returns:
        配置字典

    Raises:
        ConfigurationError: 当配置文件无效或缺失时

    Examples:
        >>> config = load_yaml_config("config.yaml", default={})
        >>> nodes = config.get('nodes', {})
    """
    file_path = Path(file_path)

    if not file_path.exists():
        if required:
            raise ConfigurationError(
                f"Required configuration file not found: {file_path}",
                error_code="CONFIG_FILE_NOT_FOUND",
                details={'file_path': str(file_path)}
            )
        return default or {}

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = yaml.safe_load(f)
            return content or {}
    except yaml.YAMLError as e:
        raise ConfigurationError(
            f"Invalid YAML in configuration file: {file_path}",
            error_code="INVALID_YAML",
            details={'file_path': str(file_path), 'yaml_error': str(e)},
            original_exception=e
        )
    except Exception as e:
        raise ConfigurationError(
            f"Failed to load configuration file: {file_path}",
            error_code="CONFIG_LOAD_FAILED",
            details={'file_path': str(file_path)},
            original_exception=e
        )


def save_yaml_config(data: Dict[str, Any],
                    file_path: Union[str, Path],
                    backup: bool = True) -> bool:
    """保存数据到YAML配置文件

    Args:
        data: 要保存的数据
        file_path: 目标文件路径
        backup: 是否创建备份

    Returns:
        是否保存成功

    Examples:
        >>> config = {'nodes': {'node1': {'host': '192.168.1.1'}}}
        >>> success = save_yaml_config(config, "nodes.yaml")
    """
    file_path = Path(file_path)

    # 创建备份
    if backup and file_path.exists():
        backup_path = file_path.with_suffix(f"{file_path.suffix}.backup.{int(time.time())}")
        try:
            backup_path.write_text(file_path.read_text())
        except Exception:
            pass  # 备份失败不阻止保存操作

    try:
        # 确保目录存在
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True, indent=2)
        return True
    except Exception:
        return False


def ensure_directory(path: Union[str, Path]) -> Path:
    """确保目录存在

    Args:
        path: 目录路径

    Returns:
        Path对象

    Examples:
        >>> logs_dir = ensure_directory("logs")
        >>> result_dir = ensure_directory(Path("results/2024"))
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def format_timestamp(timestamp: Optional[float] = None, format_str: str = "%Y%m%d_%H%M%S") -> str:
    """格式化时间戳

    Args:
        timestamp: Unix时间戳，None表示当前时间
        format_str: 格式字符串

    Returns:
        格式化的时间字符串

    Examples:
        >>> now_str = format_timestamp()
        >>> print(now_str)
        '20240315_142305'
    """
    if timestamp is None:
        timestamp = time.time()
    return datetime.fromtimestamp(timestamp).strftime(format_str)


def parse_size_string(size_str: str) -> int:
    """解析大小字符串为字节数

    Args:
        size_str: 大小字符串，如 "1GB", "512MB", "1024KB"

    Returns:
        字节数

    Examples:
        >>> bytes_count = parse_size_string("1.5GB")
        >>> print(bytes_count)
        1610612736
    """
    size_str = size_str.strip().upper()
    multipliers = {
        'B': 1,
        'KB': 1024,
        'MB': 1024 ** 2,
        'GB': 1024 ** 3,
        'TB': 1024 ** 4
    }

    for suffix, multiplier in multipliers.items():
        if size_str.endswith(suffix):
            number_part = size_str[:-len(suffix)].strip()
            try:
                return int(float(number_part) * multiplier)
            except ValueError:
                break

    # 如果没有后缀，假设是字节
    try:
        return int(float(size_str))
    except ValueError:
        raise ValueError(f"Invalid size string: {size_str}")


def format_size(bytes_count: int, precision: int = 1) -> str:
    """格式化字节数为可读的大小字符串

    Args:
        bytes_count: 字节数
        precision: 小数点精度

    Returns:
        格式化的大小字符串

    Examples:
        >>> size_str = format_size(1610612736)
        >>> print(size_str)
        '1.5 GB'
    """
    if bytes_count == 0:
        return "0 B"

    units = ['B', 'KB', 'MB', 'GB', 'TB']
    for i, unit in enumerate(units):
        if bytes_count < 1024 ** (i + 1) or i == len(units) - 1:
            size = bytes_count / (1024 ** i)
            if size == int(size):
                return f"{int(size)} {unit}"
            else:
                return f"{size:.{precision}f} {unit}"

    return f"{bytes_count} B"


def deep_merge_dicts(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Any]:
    """深度合并两个字典

    Args:
        dict1: 第一个字典
        dict2: 第二个字典（优先级更高）

    Returns:
        合并后的字典

    Examples:
        >>> base = {'a': 1, 'b': {'x': 1, 'y': 2}}
        >>> update = {'b': {'y': 3, 'z': 4}, 'c': 5}
        >>> result = deep_merge_dicts(base, update)
        >>> print(result)
        {'a': 1, 'b': {'x': 1, 'y': 3, 'z': 4}, 'c': 5}
    """
    result = dict1.copy()

    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge_dicts(result[key], value)
        else:
            result[key] = value

    return result


def validate_required_fields(data: Dict[str, Any],
                            required_fields: List[str],
                            context: str = "configuration") -> None:
    """验证必需字段

    Args:
        data: 要验证的数据字典
        required_fields: 必需字段列表
        context: 验证上下文描述

    Raises:
        ConfigurationError: 当缺少必需字段时

    Examples:
        >>> config = {'host': '192.168.1.1', 'username': 'admin'}
        >>> validate_required_fields(config, ['host', 'username', 'password'])
        # Raises ConfigurationError for missing 'password'
    """
    missing_fields = [field for field in required_fields if field not in data]

    if missing_fields:
        raise ConfigurationError(
            f"Missing required fields in {context}: {', '.join(missing_fields)}",
            error_code="MISSING_REQUIRED_FIELDS",
            details={
                'missing_fields': missing_fields,
                'context': context,
                'available_fields': list(data.keys())
            }
        )


def sanitize_filename(filename: str, replacement: str = "_") -> str:
    """清理文件名，移除非法字符

    Args:
        filename: 原始文件名
        replacement: 替换字符

    Returns:
        清理后的文件名

    Examples:
        >>> clean_name = sanitize_filename("test:scenario*1.txt")
        >>> print(clean_name)
        'test_scenario_1.txt'
    """
    import re
    # 移除或替换非法字符
    illegal_chars = r'[<>:"/\\|?*\x00-\x1f]'
    clean_name = re.sub(illegal_chars, replacement, filename)

    # 移除前后空格和点
    clean_name = clean_name.strip(' .')

    # 确保不是空字符串
    if not clean_name:
        clean_name = "unnamed"

    return clean_name


class ContextTimer:
    """上下文管理器形式的计时器

    Examples:
        >>> with ContextTimer() as timer:
        ...     time.sleep(1)
        >>> print(f"Operation took {timer.elapsed:.2f} seconds")
    """

    def __init__(self, logger: Optional[logging.Logger] = None, description: str = "Operation"):
        self.logger = logger
        self.description = description
        self.start_time = None
        self.end_time = None

    def __enter__(self):
        self.start_time = time.time()
        if self.logger:
            self.logger.info(f"{self.description} started")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        if self.logger:
            status = "completed" if exc_type is None else "failed"
            self.logger.info(f"{self.description} {status} in {self.elapsed:.2f} seconds")

    @property
    def elapsed(self) -> float:
        """获取经过的时间（秒）"""
        if self.start_time is None:
            return 0.0
        end_time = self.end_time if self.end_time is not None else time.time()
        return end_time - self.start_time


def get_environment_variable(name: str,
                           default: Optional[str] = None,
                           required: bool = False,
                           type_cast: Optional[Callable] = None) -> Any:
    """获取环境变量

    Args:
        name: 环境变量名称
        default: 默认值
        required: 是否必需
        type_cast: 类型转换函数

    Returns:
        环境变量值

    Raises:
        ConfigurationError: 当必需的环境变量不存在时

    Examples:
        >>> port = get_environment_variable("PORT", "8080", type_cast=int)
        >>> debug = get_environment_variable("DEBUG", "false", type_cast=lambda x: x.lower() == "true")
    """
    value = os.environ.get(name, default)

    if required and value is None:
        raise ConfigurationError(
            f"Required environment variable not set: {name}",
            error_code="MISSING_ENV_VAR",
            details={'variable_name': name}
        )

    if value is not None and type_cast is not None:
        try:
            value = type_cast(value)
        except (ValueError, TypeError) as e:
            raise ConfigurationError(
                f"Failed to convert environment variable {name}='{value}' to required type",
                error_code="ENV_VAR_TYPE_ERROR",
                details={'variable_name': name, 'value': value, 'type_cast': str(type_cast)},
                original_exception=e
            )

    return value