"""
统一异常处理工具
提供装饰器和工具函数，标准化整个代码库的异常处理模式
"""

import functools
import logging
import traceback
from typing import Type, Callable, Optional, Any, Dict, Union, List
from contextlib import contextmanager

from ..playbook.exceptions import (
    PlaybookException, NodeOperationError, NodeConnectionError, NodeTimeoutError,
    ScenarioError, ScenarioExecutionError, DockerError, DockerComposeError,
    TestExecutionError, ResultCollectionError, HealthCheckError,
    wrap_exception
)


class ExceptionHandler:
    """统一异常处理器"""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        
        # 标准异常映射
        self.exception_mapping = {
            # 网络连接异常
            ConnectionError: NodeConnectionError,
            TimeoutError: NodeTimeoutError,
            OSError: NodeOperationError,
            
            # 文件系统异常
            FileNotFoundError: TestExecutionError,
            PermissionError: TestExecutionError,
            IOError: TestExecutionError,
            
            # Docker异常
            # 可以根据需要添加特定的Docker异常映射
        }
    
    def handle_exception(self, 
                        exception: Exception,
                        context: str = "operation",
                        node_name: Optional[str] = None,
                        scenario_name: Optional[str] = None,
                        reraise: bool = True) -> Optional[PlaybookException]:
        """
        统一异常处理逻辑
        
        Args:
            exception: 原始异常
            context: 操作上下文描述
            node_name: 节点名称（如果相关）
            scenario_name: 场景名称（如果相关）
            reraise: 是否重新抛出异常
            
        Returns:
            包装后的异常（如果不重新抛出）
        """
        # 如果已经是Playbook异常，直接处理
        if isinstance(exception, PlaybookException):
            self.logger.error(f"Playbook error in {context}: {exception}")
            if reraise:
                raise exception
            return exception
        
        # 映射标准异常到Playbook异常
        playbook_exception = self._map_exception(
            exception, context, node_name, scenario_name
        )
        
        # 记录错误
        self.logger.error(
            f"Exception in {context}: {type(exception).__name__}: {exception}",
            extra={
                'node_name': node_name,
                'scenario_name': scenario_name,
                'original_exception': str(exception),
                'exception_type': type(exception).__name__,
                'traceback': traceback.format_exc()
            }
        )
        
        if reraise:
            raise playbook_exception
        return playbook_exception
    
    def _map_exception(self,
                      exception: Exception,
                      context: str,
                      node_name: Optional[str],
                      scenario_name: Optional[str]) -> PlaybookException:
        """将标准异常映射为Playbook异常"""
        
        exception_type = type(exception)
        
        # 查找直接映射
        if exception_type in self.exception_mapping:
            target_class = self.exception_mapping[exception_type]
            
            # 构建参数
            if issubclass(target_class, NodeOperationError) and node_name:
                return target_class(node_name, f"{context}: {str(exception)}")
            elif issubclass(target_class, ScenarioError) and scenario_name:
                return target_class(scenario_name, f"{context}: {str(exception)}")
            else:
                return target_class(f"{context}: {str(exception)}")
        
        # 基于异常类型分类
        if isinstance(exception, (ConnectionError, OSError)) and node_name:
            return NodeConnectionError(node_name, "unknown", str(exception))
        elif isinstance(exception, TimeoutError) and node_name:
            return NodeTimeoutError(node_name, context, 0)  # timeout value unknown
        elif scenario_name:
            return ScenarioExecutionError(scenario_name, context, str(exception))
        
        # 默认包装为基础异常
        return PlaybookException(
            f"{context} failed: {str(exception)}",
            error_code=f"{exception_type.__name__}_IN_{context.upper().replace(' ', '_')}",
            details={'context': context, 'node_name': node_name, 'scenario_name': scenario_name},
            original_exception=exception
        )


# 全局异常处理器实例
_global_handler = ExceptionHandler()


def with_exception_handling(context: str = "operation",
                          node_name: Optional[str] = None,
                          scenario_name: Optional[str] = None,
                          return_on_error: Any = None,
                          log_level: str = "error"):
    """
    异常处理装饰器
    
    Args:
        context: 操作上下文描述
        node_name: 节点名称（如果相关）
        scenario_name: 场景名称（如果相关）
        return_on_error: 发生异常时的返回值
        log_level: 日志级别
    
    Examples:
        @with_exception_handling("node connectivity test", node_name="node1")
        def test_node_connection(node):
            # 可能抛出异常的代码
            pass
            
        @with_exception_handling("scenario execution", return_on_error=False)
        def execute_scenario(scenario):
            # 场景执行代码
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # 尝试从参数中获取上下文信息
                actual_node_name = node_name
                actual_scenario_name = scenario_name
                
                # 如果装饰器参数中没有提供，尝试从函数参数中提取
                if not actual_node_name:
                    # 常见的参数名模式
                    for arg_name in ['node_name', 'node', 'target_node']:
                        if arg_name in kwargs:
                            actual_node_name = kwargs[arg_name]
                            break
                    
                    # 或者从位置参数中推断（假设第一个参数可能是node对象）
                    if args and hasattr(args[0], 'name'):
                        actual_node_name = getattr(args[0], 'name', None)
                
                if not actual_scenario_name:
                    for arg_name in ['scenario_name', 'scenario']:
                        if arg_name in kwargs:
                            scenario_arg = kwargs[arg_name]
                            if isinstance(scenario_arg, str):
                                actual_scenario_name = scenario_arg
                            elif hasattr(scenario_arg, 'name'):
                                actual_scenario_name = getattr(scenario_arg, 'name')
                            break
                
                # 处理异常
                if return_on_error is not None:
                    _global_handler.handle_exception(
                        e, context, actual_node_name, actual_scenario_name, reraise=False
                    )
                    return return_on_error
                else:
                    _global_handler.handle_exception(
                        e, context, actual_node_name, actual_scenario_name, reraise=True
                    )
        
        return wrapper
    return decorator


@contextmanager
def exception_context(context: str,
                     node_name: Optional[str] = None,
                     scenario_name: Optional[str] = None,
                     reraise: bool = True):
    """
    异常处理上下文管理器
    
    Args:
        context: 操作上下文描述
        node_name: 节点名称
        scenario_name: 场景名称
        reraise: 是否重新抛出异常
    
    Examples:
        with exception_context("Docker service startup", node_name="node1"):
            docker_manager.start_services()
            
        with exception_context("Result collection", reraise=False) as ctx:
            collect_results()
            if ctx.exception:
                # 处理异常但不中断流程
                pass
    """
    class ContextManager:
        def __init__(self):
            self.exception = None
    
    ctx = ContextManager()
    
    try:
        yield ctx
    except Exception as e:
        ctx.exception = _global_handler.handle_exception(
            e, context, node_name, scenario_name, reraise=reraise
        )


def safe_execute(func: Callable,
                args: tuple = (),
                kwargs: dict = None,
                context: str = "function execution",
                default_return: Any = None,
                node_name: Optional[str] = None,
                scenario_name: Optional[str] = None) -> Any:
    """
    安全执行函数，捕获并处理异常
    
    Args:
        func: 要执行的函数
        args: 位置参数
        kwargs: 关键字参数
        context: 操作上下文
        default_return: 异常时的默认返回值
        node_name: 节点名称
        scenario_name: 场景名称
    
    Returns:
        函数返回值或默认值
        
    Examples:
        result = safe_execute(
            risky_function,
            args=(param1, param2),
            context="data processing",
            default_return={}
        )
    """
    kwargs = kwargs or {}
    
    try:
        return func(*args, **kwargs)
    except Exception as e:
        _global_handler.handle_exception(
            e, context, node_name, scenario_name, reraise=False
        )
        return default_return


def collect_exceptions(operations: List[Callable],
                      context: str = "batch operation",
                      continue_on_error: bool = True) -> Dict[str, Any]:
    """
    批量执行操作并收集异常
    
    Args:
        operations: 要执行的操作列表
        context: 操作上下文
        continue_on_error: 是否在错误时继续执行
    
    Returns:
        包含结果和异常的字典
    
    Examples:
        operations = [
            lambda: test_node_1(),
            lambda: test_node_2(),
            lambda: test_node_3(),
        ]
        
        results = collect_exceptions(operations, "node testing")
        for i, result in enumerate(results['results']):
            if result['success']:
                print(f"Operation {i} succeeded: {result['value']}")
            else:
                print(f"Operation {i} failed: {result['exception']}")
    """
    results = {
        'results': [],
        'success_count': 0,
        'error_count': 0,
        'exceptions': []
    }
    
    for i, operation in enumerate(operations):
        try:
            value = operation()
            results['results'].append({
                'success': True,
                'value': value,
                'exception': None
            })
            results['success_count'] += 1
        except Exception as e:
            playbook_exception = _global_handler.handle_exception(
                e, f"{context} (operation {i})", reraise=False
            )
            
            results['results'].append({
                'success': False,
                'value': None,
                'exception': playbook_exception
            })
            results['error_count'] += 1
            results['exceptions'].append(playbook_exception)
            
            if not continue_on_error:
                break
    
    return results


def retry_with_exponential_backoff(max_retries: int = 3,
                                  base_delay: float = 1.0,
                                  max_delay: float = 60.0,
                                  backoff_factor: float = 2.0,
                                  exceptions_to_retry: tuple = (Exception,)):
    """
    带指数退避的重试装饰器
    
    Args:
        max_retries: 最大重试次数
        base_delay: 基础延迟时间（秒）
        max_delay: 最大延迟时间（秒）
        backoff_factor: 退避因子
        exceptions_to_retry: 需要重试的异常类型
    
    Examples:
        @retry_with_exponential_backoff(max_retries=3, base_delay=2.0)
        def unstable_network_operation():
            # 可能因网络问题失败的操作
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions_to_retry as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        # 最后一次尝试失败，重新抛出异常
                        raise
                    
                    # 计算延迟时间
                    delay = min(base_delay * (backoff_factor ** attempt), max_delay)
                    
                    logger = logging.getLogger(func.__module__)
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries + 1} failed for {func.__name__}: {e}. "
                        f"Retrying in {delay:.1f} seconds..."
                    )
                    
                    import time
                    time.sleep(delay)
            
            # 这行代码理论上不会执行，但为了类型检查
            if last_exception:
                raise last_exception
        
        return wrapper
    return decorator


# 便捷函数：为特定组件创建异常处理器
def create_node_exception_handler(node_name: str) -> ExceptionHandler:
    """为特定节点创建异常处理器"""
    handler = ExceptionHandler()
    # 可以添加节点特定的配置
    return handler


def create_scenario_exception_handler(scenario_name: str) -> ExceptionHandler:
    """为特定场景创建异常处理器"""
    handler = ExceptionHandler()
    # 可以添加场景特定的配置
    return handler


# 预定义的常用装饰器
node_operation = functools.partial(with_exception_handling, context="node operation")
scenario_execution = functools.partial(with_exception_handling, context="scenario execution")
docker_operation = functools.partial(with_exception_handling, context="docker operation")
result_collection = functools.partial(with_exception_handling, context="result collection")
health_check = functools.partial(with_exception_handling, context="health check")