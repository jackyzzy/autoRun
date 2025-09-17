"""配置验证和安全检查模块

提供配置文件的结构验证、安全检查和敏感信息检测功能。

主要功能:
    - YAML/JSON配置文件格式验证
    - 必需字段和类型检查
    - 敏感信息泄露检测
    - 配置项合理性验证
    - 环境变量引用验证
    - 文件权限安全检查

典型用法:
    >>> validator = ConfigValidator()
    >>> result = validator.validate_nodes_config("config/nodes.yaml")
    >>> if not result.is_valid:
    ...     for error in result.errors:
    ...         print(f"Error: {error}")
"""

import re
import os
import stat
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass
from enum import Enum
import yaml
import ipaddress

from .common import load_yaml_config
from ..playbook.exceptions import ConfigurationError, ValidationError


class SecurityLevel(Enum):
    """安全级别枚举"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class ValidationResult:
    """验证结果数据类"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    security_issues: List[Dict[str, Any]]
    suggestions: List[str]

    def add_error(self, message: str):
        """添加错误信息"""
        self.errors.append(message)
        self.is_valid = False

    def add_warning(self, message: str):
        """添加警告信息"""
        self.warnings.append(message)

    def add_security_issue(self, level: SecurityLevel, message: str,
                          field: str = None, recommendation: str = None):
        """添加安全问题"""
        issue = {
            'level': level.value,
            'message': message,
            'field': field,
            'recommendation': recommendation
        }
        self.security_issues.append(issue)

        if level in [SecurityLevel.HIGH, SecurityLevel.CRITICAL]:
            self.is_valid = False

    def add_suggestion(self, message: str):
        """添加建议"""
        self.suggestions.append(message)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'is_valid': self.is_valid,
            'errors': self.errors,
            'warnings': self.warnings,
            'security_issues': self.security_issues,
            'suggestions': self.suggestions,
            'summary': {
                'total_errors': len(self.errors),
                'total_warnings': len(self.warnings),
                'total_security_issues': len(self.security_issues),
                'critical_issues': len([
                    issue for issue in self.security_issues
                    if issue['level'] == SecurityLevel.CRITICAL.value
                ])
            }
        }


class ConfigValidator:
    """配置验证器

    提供全面的配置文件验证功能，包括格式、安全性和合理性检查。
    """

    # 敏感信息检测模式
    SENSITIVE_PATTERNS = {
        'password': re.compile(r'(?i)(password|passwd|pwd|secret|key|token)', re.IGNORECASE),
        'email': re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
        'ip_address': re.compile(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b'),
        'private_key': re.compile(r'-----BEGIN.*PRIVATE KEY-----'),
        'api_key': re.compile(r'(?i)(api[_-]?key|apikey|access[_-]?token)')
    }

    # 弱密码检测
    WEAK_PASSWORD_PATTERNS = [
        re.compile(r'^(123456|password|admin|root|guest)$', re.IGNORECASE),
        re.compile(r'^.{1,7}$'),  # 太短
        re.compile(r'^[a-z]+$'),  # 只有小写字母
        re.compile(r'^[0-9]+$'),  # 只有数字
    ]

    def __init__(self):
        self.result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            security_issues=[],
            suggestions=[]
        )

    def validate_file_permissions(self, file_path: Union[str, Path]) -> ValidationResult:
        """验证文件权限

        Args:
            file_path: 文件路径

        Returns:
            验证结果
        """
        self.result = ValidationResult(True, [], [], [], [])
        file_path = Path(file_path)

        if not file_path.exists():
            self.result.add_error(f"File not found: {file_path}")
            return self.result

        try:
            file_stat = file_path.stat()
            file_mode = stat.filemode(file_stat.st_mode)

            # 检查是否对其他用户可读
            if file_stat.st_mode & stat.S_IROTH:
                self.result.add_security_issue(
                    SecurityLevel.HIGH,
                    f"Configuration file {file_path} is readable by others",
                    recommendation="Run: chmod 600 {file_path}"
                )

            # 检查是否对其他用户可写
            if file_stat.st_mode & stat.S_IWOTH:
                self.result.add_security_issue(
                    SecurityLevel.CRITICAL,
                    f"Configuration file {file_path} is writable by others",
                    recommendation="Run: chmod 600 {file_path}"
                )

            # 检查是否对组可写
            if file_stat.st_mode & stat.S_IWGRP:
                self.result.add_security_issue(
                    SecurityLevel.MEDIUM,
                    f"Configuration file {file_path} is writable by group",
                    recommendation="Run: chmod 640 {file_path}"
                )

            # 推荐权限
            recommended_mode = 0o600  # rw-------
            if (file_stat.st_mode & 0o777) != recommended_mode:
                self.result.add_suggestion(
                    f"Consider setting stricter permissions: chmod 600 {file_path}"
                )

        except OSError as e:
            self.result.add_error(f"Failed to check file permissions: {e}")

        return self.result

    def check_sensitive_data(self, data: Dict[str, Any], path: str = "") -> None:
        """检查配置数据中的敏感信息

        Args:
            data: 配置数据字典
            path: 当前路径（用于错误报告）
        """
        if isinstance(data, dict):
            for key, value in data.items():
                current_path = f"{path}.{key}" if path else key

                # 检查键名是否包含敏感词
                if self.SENSITIVE_PATTERNS['password'].search(key):
                    if isinstance(value, str) and not value.startswith('${'):
                        # 检查是否是明文敏感信息
                        if not value.startswith('${') and len(value) > 0:
                            self.result.add_security_issue(
                                SecurityLevel.HIGH,
                                f"Potential plaintext sensitive data in field: {current_path}",
                                field=current_path,
                                recommendation="Use environment variables: ${VARIABLE_NAME}"
                            )

                        # 检查弱密码
                        if isinstance(value, str):
                            for pattern in self.WEAK_PASSWORD_PATTERNS:
                                if pattern.match(value):
                                    self.result.add_security_issue(
                                        SecurityLevel.MEDIUM,
                                        f"Weak password detected in field: {current_path}",
                                        field=current_path,
                                        recommendation="Use a stronger password"
                                    )
                                    break

                # 检查值中的敏感信息
                if isinstance(value, str):
                    # 检查私钥
                    if self.SENSITIVE_PATTERNS['private_key'].search(value):
                        self.result.add_security_issue(
                            SecurityLevel.CRITICAL,
                            f"Private key found in configuration: {current_path}",
                            field=current_path,
                            recommendation="Store private keys in separate files with proper permissions"
                        )

                    # 检查邮箱地址
                    if self.SENSITIVE_PATTERNS['email'].search(value):
                        self.result.add_warning(f"Email address found in configuration: {current_path}")

                # 递归检查嵌套数据
                if isinstance(value, (dict, list)):
                    self.check_sensitive_data(value, current_path)

        elif isinstance(data, list):
            for i, item in enumerate(data):
                current_path = f"{path}[{i}]" if path else f"[{i}]"
                if isinstance(item, (dict, list)):
                    self.check_sensitive_data(item, current_path)

    def validate_environment_variables(self, data: Dict[str, Any], path: str = "") -> None:
        """验证环境变量引用

        Args:
            data: 配置数据
            path: 当前路径
        """
        env_var_pattern = re.compile(r'\$\{([^}]+)\}')

        def check_value(value: Any, current_path: str):
            if isinstance(value, str):
                matches = env_var_pattern.findall(value)
                for var_name in matches:
                    # 检查环境变量是否存在
                    if var_name not in os.environ:
                        self.result.add_warning(
                            f"Environment variable ${{{var_name}}} not set, referenced in {current_path}"
                        )

                    # 检查敏感变量命名规范
                    if any(pattern in var_name.lower() for pattern in ['password', 'secret', 'key', 'token']):
                        if not var_name.isupper():
                            self.result.add_suggestion(
                                f"Consider using uppercase for sensitive environment variable: {var_name}"
                            )

        def traverse(obj: Any, path: str):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    current_path = f"{path}.{key}" if path else key
                    check_value(value, current_path)
                    traverse(value, current_path)
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    current_path = f"{path}[{i}]" if path else f"[{i}]"
                    check_value(item, current_path)
                    traverse(item, current_path)

        traverse(data, path)

    def validate_network_config(self, config: Dict[str, Any]) -> None:
        """验证网络配置

        Args:
            config: 网络相关配置
        """
        if 'host' in config:
            host = config['host']

            # 验证IP地址格式
            try:
                ip = ipaddress.ip_address(host)
                # 检查是否使用了不安全的IP地址
                if ip.is_loopback:
                    self.result.add_warning("Using loopback address for remote host")
                elif ip.is_private:
                    self.result.add_suggestion("Using private IP address - ensure network accessibility")
            except ValueError:
                # 可能是域名，检查格式
                if not re.match(r'^[a-zA-Z0-9]([a-zA-Z0-9\-\.]*[a-zA-Z0-9])?$', host):
                    self.result.add_error(f"Invalid hostname format: {host}")

        # 验证端口配置
        if 'port' in config:
            port = config['port']
            if not isinstance(port, int) or not (1 <= port <= 65535):
                self.result.add_error(f"Invalid port number: {port}")
            elif port < 1024:
                self.result.add_warning(f"Using privileged port {port} - ensure proper permissions")

    def validate_nodes_config(self, config_file: Union[str, Path]) -> ValidationResult:
        """验证节点配置文件

        Args:
            config_file: 节点配置文件路径

        Returns:
            验证结果
        """
        self.result = ValidationResult(True, [], [], [], [])

        # 检查文件权限
        perm_result = self.validate_file_permissions(config_file)
        self.result.errors.extend(perm_result.errors)
        self.result.warnings.extend(perm_result.warnings)
        self.result.security_issues.extend(perm_result.security_issues)

        try:
            # 加载配置
            config = load_yaml_config(config_file, required=True)

            # 检查顶级结构
            if 'nodes' not in config:
                self.result.add_error("Missing 'nodes' section in configuration")
                return self.result

            nodes = config['nodes']
            if not isinstance(nodes, dict):
                self.result.add_error("'nodes' section must be a dictionary")
                return self.result

            if not nodes:
                self.result.add_warning("No nodes defined in configuration")

            # 验证每个节点
            for node_name, node_config in nodes.items():
                self._validate_single_node(node_name, node_config)

            # 检查敏感数据
            self.check_sensitive_data(config)

            # 检查环境变量
            self.validate_environment_variables(config)

        except Exception as e:
            self.result.add_error(f"Failed to load or parse configuration: {e}")

        return self.result

    def _validate_single_node(self, node_name: str, node_config: Dict[str, Any]) -> None:
        """验证单个节点配置

        Args:
            node_name: 节点名称
            node_config: 节点配置
        """
        # 必需字段检查
        required_fields = ['host', 'username']
        for field in required_fields:
            if field not in node_config:
                self.result.add_error(f"Node '{node_name}' missing required field: {field}")

        # 认证配置检查
        if 'password' not in node_config and 'key_filename' not in node_config:
            self.result.add_error(
                f"Node '{node_name}' must have either 'password' or 'key_filename' for authentication"
            )

        # 网络配置验证
        self.validate_network_config(node_config)

        # 检查启用状态
        if 'enabled' in node_config and not isinstance(node_config['enabled'], bool):
            self.result.add_error(f"Node '{node_name}' 'enabled' field must be boolean")

        # 检查标签
        if 'tags' in node_config:
            tags = node_config['tags']
            if not isinstance(tags, list):
                self.result.add_error(f"Node '{node_name}' 'tags' field must be a list")
            elif tags and not all(isinstance(tag, str) for tag in tags):
                self.result.add_error(f"Node '{node_name}' all tags must be strings")

        # 检查角色
        if 'role' in node_config:
            role = node_config['role']
            valid_roles = ['master', 'worker', 'coordinator', 'observer']
            if role not in valid_roles:
                self.result.add_warning(
                    f"Node '{node_name}' has non-standard role '{role}'. "
                    f"Consider using one of: {', '.join(valid_roles)}"
                )

    def validate_scenarios_config(self, config_file: Union[str, Path]) -> ValidationResult:
        """验证场景配置文件

        Args:
            config_file: 场景配置文件路径

        Returns:
            验证结果
        """
        self.result = ValidationResult(True, [], [], [], [])

        # 检查文件权限
        perm_result = self.validate_file_permissions(config_file)
        self.result.errors.extend(perm_result.errors)
        self.result.warnings.extend(perm_result.warnings)
        self.result.security_issues.extend(perm_result.security_issues)

        try:
            config = load_yaml_config(config_file, required=True)

            # 检查执行配置
            if 'execution' in config:
                execution = config['execution']

                # 检查场景根目录
                if 'scenarios_root' in execution:
                    scenarios_root = Path(execution['scenarios_root'])
                    if not scenarios_root.exists():
                        self.result.add_error(f"Scenarios root directory not found: {scenarios_root}")

                # 检查执行模式
                execution_mode = execution.get('execution_mode', 'auto')
                valid_modes = ['auto', 'directory', 'custom']
                if execution_mode not in valid_modes:
                    self.result.add_error(
                        f"Invalid execution_mode '{execution_mode}'. "
                        f"Must be one of: {', '.join(valid_modes)}"
                    )

                # 检查自定义顺序配置
                if execution_mode == 'custom' and 'custom_order' not in execution:
                    self.result.add_error("Custom execution mode requires 'custom_order' configuration")

            # 检查过滤器配置
            if 'filters' in config:
                filters = config['filters']

                # 检查标签过滤器
                for filter_type in ['include_tags', 'exclude_tags']:
                    if filter_type in filters:
                        tags = filters[filter_type]
                        if not isinstance(tags, list):
                            self.result.add_error(f"Filter '{filter_type}' must be a list")

            # 检查场景间配置
            if 'inter_scenario' in config:
                inter = config['inter_scenario']

                # 检查等待时间
                if 'wait_between_scenarios' in inter:
                    wait_time = inter['wait_between_scenarios']
                    if not isinstance(wait_time, (int, float)) or wait_time < 0:
                        self.result.add_error("'wait_between_scenarios' must be a non-negative number")

                # 检查重试配置
                if 'retry_count' in inter:
                    retry_count = inter['retry_count']
                    if not isinstance(retry_count, int) or retry_count < 0:
                        self.result.add_error("'retry_count' must be a non-negative integer")

            # 检查敏感数据和环境变量
            self.check_sensitive_data(config)
            self.validate_environment_variables(config)

        except Exception as e:
            self.result.add_error(f"Failed to load or parse scenarios configuration: {e}")

        return self.result

    def generate_security_report(self, validation_results: List[ValidationResult]) -> Dict[str, Any]:
        """生成安全报告

        Args:
            validation_results: 验证结果列表

        Returns:
            安全报告字典
        """
        all_issues = []
        total_errors = 0
        total_warnings = 0

        for result in validation_results:
            all_issues.extend(result.security_issues)
            total_errors += len(result.errors)
            total_warnings += len(result.warnings)

        # 按安全级别分组
        issues_by_level = {}
        for issue in all_issues:
            level = issue['level']
            if level not in issues_by_level:
                issues_by_level[level] = []
            issues_by_level[level].append(issue)

        # 生成建议
        recommendations = []
        if issues_by_level.get(SecurityLevel.CRITICAL.value):
            recommendations.append("Immediately address critical security issues")
        if issues_by_level.get(SecurityLevel.HIGH.value):
            recommendations.append("Review and fix high-priority security issues")
        if total_errors == 0 and total_warnings == 0:
            recommendations.append("Configuration appears secure")

        return {
            'summary': {
                'total_issues': len(all_issues),
                'critical_issues': len(issues_by_level.get(SecurityLevel.CRITICAL.value, [])),
                'high_issues': len(issues_by_level.get(SecurityLevel.HIGH.value, [])),
                'medium_issues': len(issues_by_level.get(SecurityLevel.MEDIUM.value, [])),
                'low_issues': len(issues_by_level.get(SecurityLevel.LOW.value, [])),
                'total_errors': total_errors,
                'total_warnings': total_warnings
            },
            'issues_by_level': issues_by_level,
            'recommendations': recommendations,
            'timestamp': str(Path().cwd())  # 简单的时间戳占位符
        }