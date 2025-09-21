"""
全局配置管理器
实现三层配置系统：场景配置 > 全局配置 > 系统默认值
"""

import logging
from typing import Dict, Any, Optional
from pathlib import Path

from .config_loader import ConfigLoader


class GlobalConfigManager:
    """全局配置管理器 - 三层配置系统的核心组件"""

    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.logger = logging.getLogger("playbook.global_config_manager")
        self.config_loader = ConfigLoader()

        # 加载配置
        self.global_defaults = self._load_global_defaults()
        self.system_defaults = self._get_system_defaults()

        self.logger.info(f"GlobalConfigManager initialized with config_dir: {config_dir}")
        if self.global_defaults:
            self.logger.info(f"Loaded global defaults with {len(self.global_defaults)} sections")

    def _load_global_defaults(self) -> Dict[str, Any]:
        """加载全局默认配置文件"""
        defaults_path = self.config_dir / "defaults.yaml"
        if defaults_path.exists():
            try:
                config = self.config_loader.load_yaml(str(defaults_path))
                self.logger.info(f"Loaded global defaults from {defaults_path}")
                return config
            except Exception as e:
                self.logger.warning(f"Failed to load global defaults from {defaults_path}: {e}")
        else:
            self.logger.info(f"Global defaults file not found at {defaults_path}, using system defaults only")

        return {}

    def _get_system_defaults(self) -> Dict[str, Any]:
        """获取系统内置默认值 - 作为最后防线"""
        return {
            'service_health_check': {
                'enabled': True,
                'strategy': 'standard',
                'startup_timeout': 180,  # 保守的3分钟
                'startup_grace_period': 90,  # 1.5分钟
                'check_interval': 15,
                'max_retries': 3,  # 从10降到3
                'retry_delay': 15,
                'failure_action': 'retry'
            },
            'test_execution': {
                'node': 'local',
                'script': 'run_test.sh',
                'timeout': 3600,  # 1小时，适合长时间测试
                'wait_for_all_services': True
            },
            'concurrent_execution': {
                'max_concurrent_services': 5,
                'deployment_timeout': 600,  # 10分钟
                'health_check_timeout': 300,  # 5分钟
                'max_concurrent_health_checks': 8,
                'max_workers_health_check': 6  # 替代hardcoded max_workers=4
            },
            'file_operations': {
                'upload_retries': 2,
                'verification_timeout': 10,
                'cleanup_timeout': 10,
                'hash_check_timeout': 10
            },
            'progress_display': {
                'countdown_interval_long': 10,    # >= 30s时使用
                'countdown_interval_short': 5,    # < 30s时使用
                'thread_join_timeout': 2,         # 线程等待超时
                'retry_display_delay': 2
            },
            'system_resources': {
                'max_ssh_workers': 8,
                'max_file_workers': 5,
                'connection_timeout': 30,
                'command_timeout': 300,
                'max_failure_threshold': 3
            }
        }

    def get_merged_config(self, config_type: str, scenario_config: Dict[str, Any] = None) -> Dict[str, Any]:
        """三层配置合并的核心方法

        Args:
            config_type: 配置类型 (如 'service_health_check')
            scenario_config: 场景级配置

        Returns:
            合并后的配置字典
        """
        system_defaults = self.system_defaults.get(config_type, {})
        global_defaults = self.global_defaults.get(config_type, {})
        scenario_config = scenario_config or {}

        # 三层合并：场景 > 全局 > 系统
        merged = {}
        all_keys = set(system_defaults.keys()) | set(global_defaults.keys()) | set(scenario_config.keys())

        for key in all_keys:
            if key in scenario_config and scenario_config[key] is not None:
                merged[key] = scenario_config[key]
                source = "scenario"
            elif key in global_defaults and global_defaults[key] is not None:
                merged[key] = global_defaults[key]
                source = "global"
            elif key in system_defaults:
                merged[key] = system_defaults[key]
                source = "system"
            else:
                continue

            # 调试日志（仅在详细模式下）
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(f"Config {config_type}.{key} = {merged[key]} (from {source})")

        return merged

    @staticmethod
    def merge_config(config_type: str, scenario_config: Dict[str, Any],
                    global_defaults: Dict[str, Any] = None) -> Dict[str, Any]:
        """静态方法版本的配置合并 - 用于dataclass中的from_dict方法"""
        # 创建临时实例来使用合并逻辑
        temp_manager = GlobalConfigManager()
        if global_defaults:
            temp_manager.global_defaults = global_defaults

        return temp_manager.get_merged_config(config_type, scenario_config)

    def validate_config(self, config_type: str, config: Dict[str, Any]) -> list[str]:
        """配置验证 - 返回警告列表"""
        warnings = []

        if config_type == 'service_health_check':
            warnings.extend(self._validate_service_health_check(config))
        elif config_type == 'concurrent_execution':
            warnings.extend(self._validate_concurrent_execution(config))
        elif config_type == 'test_execution':
            warnings.extend(self._validate_test_execution(config))

        return warnings

    def _validate_service_health_check(self, config: Dict[str, Any]) -> list[str]:
        """验证服务健康检查配置"""
        warnings = []

        startup_timeout = config.get('startup_timeout')
        if startup_timeout:
            if startup_timeout < 30:
                warnings.append(f"startup_timeout={startup_timeout}s may be too short for complex services")
            elif startup_timeout > 1200:
                warnings.append(f"startup_timeout={startup_timeout}s is very long, consider optimizing service startup")

        max_retries = config.get('max_retries')
        if max_retries and max_retries > 15:
            warnings.append(f"max_retries={max_retries} is high, may mask underlying issues")

        return warnings

    def _validate_concurrent_execution(self, config: Dict[str, Any]) -> list[str]:
        """验证并发执行配置"""
        warnings = []

        max_concurrent = config.get('max_concurrent_services')
        if max_concurrent and max_concurrent > 20:
            warnings.append(f"max_concurrent_services={max_concurrent} may overwhelm system resources")

        deployment_timeout = config.get('deployment_timeout')
        if deployment_timeout and deployment_timeout < 60:
            warnings.append(f"deployment_timeout={deployment_timeout}s may be too short")

        return warnings

    def _validate_test_execution(self, config: Dict[str, Any]) -> list[str]:
        """验证测试执行配置"""
        warnings = []

        timeout = config.get('timeout')
        if timeout:
            if timeout < 300:
                warnings.append(f"test timeout={timeout}s may be too short for comprehensive tests")
            elif timeout > 14400:  # 4 hours
                warnings.append(f"test timeout={timeout}s is very long, consider test optimization")

        return warnings

    def get_config_summary(self) -> Dict[str, Any]:
        """获取配置系统摘要信息"""
        return {
            'config_dir': str(self.config_dir),
            'global_defaults_loaded': bool(self.global_defaults),
            'available_config_types': list(self.system_defaults.keys()),
            'global_config_sections': list(self.global_defaults.keys()) if self.global_defaults else []
        }

    def reload_global_defaults(self) -> bool:
        """重新加载全局默认配置"""
        try:
            old_config = self.global_defaults.copy()
            self.global_defaults = self._load_global_defaults()

            if old_config != self.global_defaults:
                self.logger.info("Global defaults configuration reloaded with changes")
                return True
            else:
                self.logger.info("Global defaults configuration reloaded (no changes)")
                return False
        except Exception as e:
            self.logger.error(f"Failed to reload global defaults: {e}")
            return False