"""Playbook自定义异常类

定义系统中使用的各种异常类型，提供更好的错误分类和处理。

异常层次结构:
    PlaybookException (基础异常)
    ├── ConfigurationError (配置相关错误)
    │   ├── InvalidConfigError
    │   ├── MissingConfigError
    │   └── ValidationError
    ├── NodeOperationError (节点操作错误)
    │   ├── NodeConnectionError
    │   ├── NodeCommandError
    │   └── NodeTimeoutError
    ├── ScenarioError (场景执行错误)
    │   ├── ScenarioNotFoundError
    │   ├── ScenarioValidationError
    │   └── ScenarioExecutionError
    ├── DockerError (Docker相关错误)
    │   ├── DockerComposeError
    │   ├── ContainerError
    │   └── ServiceError
    └── TestExecutionError (测试执行错误)
        ├── BenchmarkError
        ├── ResultCollectionError
        └── HealthCheckError
"""

from typing import Optional, Dict, Any


class PlaybookException(Exception):
    """Playbook基础异常类

    所有Playbook异常的基类，提供统一的异常处理接口。

    Attributes:
        message: 错误消息
        error_code: 错误代码
        details: 错误详细信息字典
        original_exception: 原始异常对象
    """

    def __init__(self,
                 message: str,
                 error_code: Optional[str] = None,
                 details: Optional[Dict[str, Any]] = None,
                 original_exception: Optional[Exception] = None):
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.details = details or {}
        self.original_exception = original_exception
        super().__init__(self.message)

    def __str__(self) -> str:
        """返回格式化的错误消息"""
        result = f"[{self.error_code}] {self.message}"
        if self.details:
            detail_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            result += f" ({detail_str})"
        return result

    def to_dict(self) -> Dict[str, Any]:
        """将异常转换为字典格式"""
        return {
            'error_type': self.__class__.__name__,
            'error_code': self.error_code,
            'message': self.message,
            'details': self.details,
            'original_exception': str(self.original_exception) if self.original_exception else None
        }


# 配置相关异常
class ConfigurationError(PlaybookException):
    """配置相关异常基类"""
    pass


class InvalidConfigError(ConfigurationError):
    """无效配置异常"""

    def __init__(self, config_file: str, field: str, reason: str):
        super().__init__(
            f"Invalid configuration in {config_file}: {field} - {reason}",
            details={'config_file': config_file, 'field': field, 'reason': reason}
        )


class MissingConfigError(ConfigurationError):
    """缺失配置异常"""

    def __init__(self, config_file: str, required_field: str):
        super().__init__(
            f"Missing required configuration in {config_file}: {required_field}",
            details={'config_file': config_file, 'required_field': required_field}
        )


class ValidationError(ConfigurationError):
    """配置验证异常"""

    def __init__(self, validation_errors: list):
        error_summary = f"{len(validation_errors)} validation errors found"
        super().__init__(
            error_summary,
            details={'errors': validation_errors, 'count': len(validation_errors)}
        )


# 节点操作异常
class NodeOperationError(PlaybookException):
    """节点操作异常基类"""

    def __init__(self, node_name: str, message: str, **kwargs):
        super().__init__(
            f"Node operation failed on '{node_name}': {message}",
            details={'node_name': node_name, **kwargs}
        )


class NodeConnectionError(NodeOperationError):
    """节点连接异常"""

    def __init__(self, node_name: str, host: str, reason: str):
        super().__init__(
            node_name,
            f"Failed to connect to {host}: {reason}",
            host=host,
            reason=reason
        )


class NodeCommandError(NodeOperationError):
    """节点命令执行异常"""

    def __init__(self, node_name: str, command: str, exit_code: int, stderr: str):
        super().__init__(
            node_name,
            f"Command failed with exit code {exit_code}: {command}",
            command=command,
            exit_code=exit_code,
            stderr=stderr
        )


class NodeTimeoutError(NodeOperationError):
    """节点操作超时异常"""

    def __init__(self, node_name: str, operation: str, timeout: int):
        super().__init__(
            node_name,
            f"Operation '{operation}' timed out after {timeout} seconds",
            operation=operation,
            timeout=timeout
        )


# 场景相关异常
class ScenarioError(PlaybookException):
    """场景异常基类"""

    def __init__(self, scenario_name: str, message: str, **kwargs):
        super().__init__(
            f"Scenario error in '{scenario_name}': {message}",
            details={'scenario_name': scenario_name, **kwargs}
        )


class ScenarioNotFoundError(ScenarioError):
    """场景不存在异常"""

    def __init__(self, scenario_name: str, search_path: str):
        super().__init__(
            scenario_name,
            f"Scenario not found in {search_path}",
            search_path=search_path
        )


class ScenarioValidationError(ScenarioError):
    """场景验证异常"""

    def __init__(self, scenario_name: str, validation_errors: list):
        super().__init__(
            scenario_name,
            f"Scenario validation failed with {len(validation_errors)} errors",
            validation_errors=validation_errors,
            error_count=len(validation_errors)
        )


class ScenarioExecutionError(ScenarioError):
    """场景执行异常"""

    def __init__(self, scenario_name: str, step: str, reason: str):
        super().__init__(
            scenario_name,
            f"Execution failed at step '{step}': {reason}",
            step=step,
            reason=reason
        )


# Docker相关异常
class DockerError(PlaybookException):
    """Docker异常基类"""
    pass


class DockerComposeError(DockerError):
    """Docker Compose异常"""

    def __init__(self, command: str, exit_code: int, output: str, node: str = None):
        message = f"Docker Compose command failed: {command}"
        if node:
            message += f" on node {node}"
        super().__init__(
            message,
            details={
                'command': command,
                'exit_code': exit_code,
                'output': output,
                'node': node
            }
        )


class ContainerError(DockerError):
    """容器异常"""

    def __init__(self, container_name: str, operation: str, reason: str):
        super().__init__(
            f"Container '{container_name}' {operation} failed: {reason}",
            details={
                'container_name': container_name,
                'operation': operation,
                'reason': reason
            }
        )


class ServiceError(DockerError):
    """服务异常"""

    def __init__(self, service_name: str, status: str, expected_status: str):
        super().__init__(
            f"Service '{service_name}' is {status}, expected {expected_status}",
            details={
                'service_name': service_name,
                'actual_status': status,
                'expected_status': expected_status
            }
        )


# 测试执行异常
class TestExecutionError(PlaybookException):
    """测试执行异常基类"""
    pass


class BenchmarkError(TestExecutionError):
    """基准测试异常"""

    def __init__(self, test_name: str, phase: str, reason: str):
        super().__init__(
            f"Benchmark '{test_name}' failed at {phase}: {reason}",
            details={
                'test_name': test_name,
                'phase': phase,
                'reason': reason
            }
        )


class ResultCollectionError(TestExecutionError):
    """结果收集异常"""

    def __init__(self, node_name: str, file_path: str, reason: str):
        super().__init__(
            f"Failed to collect results from {node_name}:{file_path}: {reason}",
            details={
                'node_name': node_name,
                'file_path': file_path,
                'reason': reason
            }
        )


class HealthCheckError(TestExecutionError):
    """健康检查异常"""

    def __init__(self, component: str, check_type: str, details: Dict[str, Any]):
        super().__init__(
            f"Health check failed for {component}: {check_type}",
            details={
                'component': component,
                'check_type': check_type,
                **details
            }
        )


# 便捷函数
def wrap_exception(original_exception: Exception,
                  new_exception_class: type,
                  message: str,
                  **kwargs) -> PlaybookException:
    """将原始异常包装为Playbook异常

    Args:
        original_exception: 原始异常对象
        new_exception_class: 新异常类
        message: 新异常消息
        **kwargs: 传递给新异常的额外参数

    Returns:
        包装后的Playbook异常

    Examples:
        >>> try:
        ...     # 某个可能抛出异常的操作
        ...     risky_operation()
        ... except ConnectionError as e:
        ...     raise wrap_exception(e, NodeConnectionError, "node1", "192.168.1.1", str(e))
    """
    if issubclass(new_exception_class, PlaybookException):
        return new_exception_class(message, original_exception=original_exception, **kwargs)
    else:
        raise TypeError(f"{new_exception_class} is not a PlaybookException subclass")


def format_exception_chain(exception: Exception) -> str:
    """格式化异常链，显示完整的异常追踪

    Args:
        exception: 要格式化的异常

    Returns:
        格式化的异常链字符串

    Examples:
        >>> try:
        ...     raise ValueError("original error")
        ... except ValueError as e:
        ...     wrapped = wrap_exception(e, ConfigurationError, "config failed")
        ...     print(format_exception_chain(wrapped))
    """
    result = []
    current = exception

    while current:
        if isinstance(current, PlaybookException):
            result.append(f"[{current.error_code}] {current.message}")
            if current.details:
                detail_str = ", ".join(f"{k}={v}" for k, v in current.details.items())
                result.append(f"  Details: {detail_str}")
            current = current.original_exception
        else:
            result.append(f"[{current.__class__.__name__}] {str(current)}")
            current = getattr(current, '__cause__', None)

    return "\n".join(result)