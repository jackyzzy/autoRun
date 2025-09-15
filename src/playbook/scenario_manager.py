"""
场景配置管理器
处理测试场景的发现、配置、排序和管理
"""

import os
import yaml
import logging
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
import json

from ..utils.config_loader import ConfigLoader, ConfigError


@dataclass
class ServiceHealthCheck:
    """服务健康检查配置"""
    enabled: bool = True
    strategy: str = "standard"  # minimal/standard/comprehensive
    startup_timeout: int = 300
    startup_grace_period: int = 60
    check_interval: int = 15
    max_retries: int = 10
    checks: List[Dict[str, Any]] = field(default_factory=list)
    failure_action: str = "retry"  # retry/skip/abort
    retry_delay: int = 30
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ServiceHealthCheck':
        if not data:
            return cls()
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass 
class ServiceDeployment:
    """服务部署配置"""
    name: str
    compose_file: str = "docker-compose.yml"
    nodes: List[str] = field(default_factory=list)
    depends_on: List[str] = field(default_factory=list)
    health_check: ServiceHealthCheck = field(default_factory=ServiceHealthCheck)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ServiceDeployment':
        health_check_data = data.pop('health_check', {})
        service = cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
        service.health_check = ServiceHealthCheck.from_dict(health_check_data)
        return service


@dataclass
class TestExecution:
    """测试执行配置"""
    node: str = "local"
    script: str = "run_test.sh"
    timeout: int = 1800
    result_paths: List[str] = field(default_factory=list)
    wait_for_all_services: bool = True
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TestExecution':
        if not data:
            return cls()
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class DeploymentConfig:
    """部署配置"""
    services: List[ServiceDeployment] = field(default_factory=list)
    test_execution: TestExecution = field(default_factory=TestExecution)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DeploymentConfig':
        if not data:
            return cls()
        
        # 解析services
        services = []
        for service_data in data.get('services', []):
            services.append(ServiceDeployment.from_dict(service_data))
        
        # 解析test_execution
        test_execution = TestExecution.from_dict(data.get('test_execution', {}))
        
        return cls(services=services, test_execution=test_execution)


@dataclass
class ScenarioMetadata:
    """场景元数据"""
    name: str
    description: str = ""
    version: str = "1.0"
    tags: List[str] = field(default_factory=list)
    estimated_duration: int = 0  # 预估执行时间(秒)
    dependencies: List[str] = field(default_factory=list)
    resource_requirements: Dict[str, Any] = field(default_factory=dict)
    author: str = ""
    created: str = ""
    deployment: DeploymentConfig = field(default_factory=DeploymentConfig)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ScenarioMetadata':
        # 处理deployment字段
        deployment_data = data.pop('deployment', {})
        metadata = cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
        metadata.deployment = DeploymentConfig.from_dict(deployment_data)
        return metadata
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            'name': self.name,
            'description': self.description,
            'version': self.version,
            'tags': self.tags,
            'estimated_duration': self.estimated_duration,
            'dependencies': self.dependencies,
            'resource_requirements': self.resource_requirements,
            'author': self.author,
            'created': self.created
        }
        
        # 只在有部署配置时包含deployment
        if self.deployment.services or self.deployment.test_execution.node != "local":
            result['deployment'] = {
                'services': [service.__dict__ for service in self.deployment.services],
                'test_execution': self.deployment.test_execution.__dict__
            }
            
        return result


@dataclass
class Scenario:
    """测试场景"""
    name: str
    directory: str
    enabled: bool = True
    description: str = ""
    metadata: Optional[ScenarioMetadata] = None
    
    # 文件路径
    docker_compose_file: str = ""
    test_config_file: str = ""
    run_script: str = ""
    
    def __post_init__(self):
        if not self.description and self.metadata:
            self.description = self.metadata.description
    
    @property
    def full_path(self) -> Path:
        """获取场景完整路径"""
        return Path(self.directory)
    
    @property
    def is_valid(self) -> bool:
        """检查场景是否有效"""
        scenario_path = self.full_path
        return (scenario_path.exists() and 
                scenario_path.is_dir() and
                (self._find_docker_compose_file(scenario_path) is not None))
    
    def get_docker_compose_path(self) -> str:
        """获取docker-compose文件路径"""
        if self.docker_compose_file:
            return self.docker_compose_file
        
        docker_compose_file = self._find_docker_compose_file(self.full_path)
        if docker_compose_file:
            return str(docker_compose_file)
        return str(self.full_path / "docker-compose.yml")
    
    def get_test_config_path(self) -> str:
        """获取测试配置文件路径"""
        if self.test_config_file:
            return self.test_config_file
        
        # 尝试常见的配置文件名
        for filename in ["test_config.json", "test_config.yaml", "config.json"]:
            config_path = self.full_path / filename
            if config_path.exists():
                return str(config_path)
        
        return ""
    
    def get_run_script_path(self) -> str:
        """获取运行脚本路径"""
        if self.run_script:
            return self.run_script
        
        # 尝试常见的脚本名
        for filename in ["run_test.sh", "test.sh", "run.sh"]:
            script_path = self.full_path / filename
            if script_path.exists():
                return str(script_path)
        
        return ""
    
    def _find_docker_compose_file(self, directory: Path) -> Optional[Path]:
        """查找docker-compose文件，支持.yml和.yaml扩展名"""
        for ext in [".yml", ".yaml"]:
            docker_compose_file = directory / f"docker-compose{ext}"
            if docker_compose_file.exists():
                return docker_compose_file
        return None
    
    def _find_metadata_file(self, directory: Path) -> Optional[Path]:
        """查找metadata文件，支持.yml和.yaml扩展名"""
        for ext in [".yml", ".yaml"]:
            metadata_file = directory / f"metadata{ext}"
            if metadata_file.exists():
                return metadata_file
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'directory': self.directory,
            'enabled': self.enabled,
            'description': self.description,
            'docker_compose_file': self.docker_compose_file,
            'test_config_file': self.test_config_file,
            'run_script': self.run_script,
            'metadata': self.metadata.to_dict() if self.metadata else None
        }


class ScenarioDiscoveryMode:
    """场景发现模式枚举"""
    AUTO = "auto"          # 自动发现
    DIRECTORY = "directory"  # 目录排序
    CUSTOM = "custom"      # 自定义顺序


class ScenarioManager:
    """场景管理器"""
    
    def __init__(self, config_file: str = None, scenarios_root: str = None):
        self.logger = logging.getLogger("playbook.scenario_manager")
        self.config_loader = ConfigLoader()
        
        # 配置
        self.config_file = config_file
        self.scenarios_root = scenarios_root or "config/scenarios"
        
        # 场景数据
        self.scenarios: Dict[str, Scenario] = {}
        self.execution_order: List[str] = []
        self.config: Dict[str, Any] = {}
        
        # 执行配置
        self.execution_mode = ScenarioDiscoveryMode.AUTO
        self.filters = {}
        self.inter_scenario_config = {}
        
        if config_file and Path(config_file).exists():
            self.load_config()
        
        self.discover_scenarios()
    
    def load_config(self):
        """加载场景配置"""
        try:
            self.config = self.config_loader.load_yaml(self.config_file)
            
            # 加载执行配置
            execution_config = self.config.get('execution', {})
            self.scenarios_root = execution_config.get('scenarios_root', self.scenarios_root)
            self.execution_mode = execution_config.get('execution_mode', ScenarioDiscoveryMode.AUTO)
            
            # 加载过滤配置
            self.filters = self.config.get('filters', {})
            
            # 加载场景间配置
            self.inter_scenario_config = self.config.get('inter_scenario', {})
            
            self.logger.info(f"Loaded scenario configuration from {self.config_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to load scenario config: {e}")
            raise ConfigError(f"Failed to load scenario configuration: {e}")
    
    def discover_scenarios(self):
        """发现场景"""
        self.scenarios.clear()
        
        scenarios_path = Path(self.scenarios_root)
        if not scenarios_path.exists():
            self.logger.warning(f"Scenarios directory not found: {scenarios_path}")
            return
        
        # 扫描场景目录
        discovered_scenarios = []
        
        for item in scenarios_path.iterdir():
            if item.is_dir():
                scenario = self._create_scenario_from_directory(item)
                if scenario:
                    discovered_scenarios.append(scenario)
                    self.scenarios[scenario.name] = scenario
        
        self.logger.info(f"Discovered {len(discovered_scenarios)} scenarios")
        
        # 根据模式设置执行顺序
        self._set_execution_order(discovered_scenarios)
    
    def _create_scenario_from_directory(self, directory: Path) -> Optional[Scenario]:
        """从目录创建场景对象"""
        try:
            # 检查是否有必需的文件
            docker_compose_file = self._find_docker_compose_file(directory)
            if not docker_compose_file:
                self.logger.debug(f"Skipping {directory}: no docker-compose file found")
                return None
            
            # 创建基本场景信息
            scenario_name = directory.name
            scenario = Scenario(
                name=scenario_name,
                directory=str(directory)
            )
            
            # 加载元数据
            metadata_file = self._find_metadata_file(directory)
            if metadata_file:
                try:
                    metadata_config = self.config_loader.load_yaml(str(metadata_file))
                    scenario.metadata = ScenarioMetadata.from_dict(metadata_config)
                    scenario.description = scenario.metadata.description
                except Exception as e:
                    self.logger.warning(f"Failed to load metadata for {scenario_name}: {e}")
            
            self.logger.debug(f"Created scenario: {scenario_name}")
            return scenario
            
        except Exception as e:
            self.logger.error(f"Failed to create scenario from {directory}: {e}")
            return None
    
    def _find_docker_compose_file(self, directory: Path) -> Optional[Path]:
        """查找docker-compose文件，支持.yml和.yaml扩展名"""
        for ext in [".yml", ".yaml"]:
            docker_compose_file = directory / f"docker-compose{ext}"
            if docker_compose_file.exists():
                return docker_compose_file
        return None
    
    def _find_metadata_file(self, directory: Path) -> Optional[Path]:
        """查找metadata文件，支持.yml和.yaml扩展名"""
        for ext in [".yml", ".yaml"]:
            metadata_file = directory / f"metadata{ext}"
            if metadata_file.exists():
                return metadata_file
        return None
    
    def _set_execution_order(self, scenarios: List[Scenario]):
        """设置执行顺序"""
        if self.execution_mode == ScenarioDiscoveryMode.CUSTOM:
            self._set_custom_order()
        elif self.execution_mode == ScenarioDiscoveryMode.DIRECTORY:
            self._set_directory_order(scenarios)
        else:  # AUTO mode
            self._set_auto_order(scenarios)
    
    def _set_custom_order(self):
        """设置自定义顺序"""
        custom_order = self.config.get('execution', {}).get('custom_order', [])
        self.execution_order = []
        
        # In custom mode, we clear auto-discovered scenarios and rebuild from config
        custom_scenarios = {}
        
        for item in custom_order:
            if isinstance(item, dict):
                name = item.get('name')
                directory = item.get('directory')
                enabled = item.get('enabled', True)
                description = item.get('description', '')
                
                if name and directory:
                    # 创建或更新场景
                    full_directory = Path(self.scenarios_root) / directory
                    scenario = self._create_scenario_from_directory(full_directory)
                    
                    if scenario:
                        scenario.name = name
                        scenario.enabled = enabled
                        if description:
                            scenario.description = description
                        
                        custom_scenarios[name] = scenario
                        if enabled:
                            self.execution_order.append(name)
            elif isinstance(item, str):
                # 简单字符串格式
                if item in self.scenarios:
                    custom_scenarios[item] = self.scenarios[item]
                    self.execution_order.append(item)
        
        # Replace scenarios with custom configuration
        self.scenarios = custom_scenarios
        
        self.logger.info(f"Set custom execution order: {self.execution_order}")
    
    def _set_directory_order(self, scenarios: List[Scenario]):
        """按目录名排序"""
        # 按目录名排序（支持数字前缀）
        def sort_key(scenario: Scenario) -> Tuple[int, str]:
            name = Path(scenario.directory).name
            # 提取数字前缀
            import re
            match = re.match(r'^(\d+)', name)
            if match:
                return (int(match.group(1)), name)
            else:
                return (999999, name)  # 无数字前缀的排到后面
        
        sorted_scenarios = sorted(scenarios, key=sort_key)
        self.execution_order = [s.name for s in sorted_scenarios if s.enabled]
        
        self.logger.info(f"Set directory order: {self.execution_order}")
    
    def _set_auto_order(self, scenarios: List[Scenario]):
        """自动排序"""
        # 默认使用目录排序
        self._set_directory_order(scenarios)
    
    def get_execution_order(self, apply_filters: bool = True) -> List[str]:
        """获取执行顺序"""
        if not apply_filters:
            return self.execution_order.copy()
        
        # 应用过滤器
        filtered_order = []
        
        for scenario_name in self.execution_order:
            scenario = self.scenarios.get(scenario_name)
            if not scenario or not scenario.enabled:
                continue
            
            # 检查过滤条件
            if self._should_include_scenario(scenario):
                filtered_order.append(scenario_name)
        
        return filtered_order
    
    def _should_include_scenario(self, scenario: Scenario) -> bool:
        """检查场景是否应该包含在执行中"""
        # only_scenarios 过滤
        only_scenarios = self.filters.get('only_scenarios', [])
        if only_scenarios and scenario.name not in only_scenarios:
            return False
        
        # skip_scenarios 过滤
        skip_scenarios = self.filters.get('skip_scenarios', [])
        if scenario.name in skip_scenarios:
            return False
        
        # 标签过滤
        if scenario.metadata:
            include_tags = self.filters.get('include_tags', [])
            if include_tags and not any(tag in scenario.metadata.tags for tag in include_tags):
                return False
            
            exclude_tags = self.filters.get('exclude_tags', [])
            if exclude_tags and any(tag in scenario.metadata.tags for tag in exclude_tags):
                return False
        
        return True
    
    def get_scenario(self, name: str) -> Optional[Scenario]:
        """获取场景"""
        return self.scenarios.get(name)
    
    def get_scenarios(self, enabled_only: bool = True) -> List[Scenario]:
        """获取场景列表"""
        scenarios = list(self.scenarios.values())
        if enabled_only:
            scenarios = [s for s in scenarios if s.enabled]
        return scenarios
    
    def enable_scenario(self, name: str) -> bool:
        """启用场景"""
        scenario = self.scenarios.get(name)
        if scenario:
            scenario.enabled = True
            self.logger.info(f"Enabled scenario: {name}")
            return True
        return False
    
    def disable_scenario(self, name: str) -> bool:
        """禁用场景"""
        scenario = self.scenarios.get(name)
        if scenario:
            scenario.enabled = False
            self.logger.info(f"Disabled scenario: {name}")
            return True
        return False
    
    def add_scenario(self, name: str, directory: str, **kwargs) -> Scenario:
        """添加场景"""
        scenario = Scenario(name=name, directory=directory, **kwargs)
        self.scenarios[name] = scenario
        
        # 添加到执行顺序
        if scenario.enabled and name not in self.execution_order:
            self.execution_order.append(name)
        
        self.logger.info(f"Added scenario: {name}")
        return scenario
    
    def remove_scenario(self, name: str) -> bool:
        """移除场景"""
        if name in self.scenarios:
            del self.scenarios[name]
            if name in self.execution_order:
                self.execution_order.remove(name)
            self.logger.info(f"Removed scenario: {name}")
            return True
        return False
    
    def reorder_scenarios(self, new_order: List[str]) -> bool:
        """重新排序场景"""
        # 验证所有场景存在
        for name in new_order:
            if name not in self.scenarios:
                self.logger.error(f"Scenario not found: {name}")
                return False
        
        self.execution_order = new_order.copy()
        self.logger.info(f"Reordered scenarios: {new_order}")
        return True
    
    def validate_scenarios(self) -> Dict[str, List[str]]:
        """验证所有场景"""
        results = {
            'valid': [],
            'invalid': [],
            'missing_dependencies': []
        }
        
        for name, scenario in self.scenarios.items():
            if not scenario.is_valid:
                results['invalid'].append(name)
                continue
            
            # 检查依赖
            if scenario.metadata and scenario.metadata.dependencies:
                missing_deps = []
                for dep in scenario.metadata.dependencies:
                    if dep not in self.scenarios:
                        missing_deps.append(dep)
                
                if missing_deps:
                    results['missing_dependencies'].append(f"{name}: {missing_deps}")
                    continue
            
            results['valid'].append(name)
        
        return results
    
    def get_summary(self) -> Dict[str, Any]:
        """获取场景摘要信息"""
        total_scenarios = len(self.scenarios)
        enabled_scenarios = len([s for s in self.scenarios.values() if s.enabled])
        
        # 统计标签
        all_tags = set()
        for scenario in self.scenarios.values():
            if scenario.metadata:
                all_tags.update(scenario.metadata.tags)
        
        # 计算总预估时间
        total_estimated_time = 0
        for name in self.get_execution_order():
            scenario = self.scenarios.get(name)
            if scenario and scenario.metadata:
                total_estimated_time += scenario.metadata.estimated_duration
        
        return {
            'total_scenarios': total_scenarios,
            'enabled_scenarios': enabled_scenarios,
            'disabled_scenarios': total_scenarios - enabled_scenarios,
            'execution_mode': self.execution_mode,
            'execution_order': self.get_execution_order(),
            'all_tags': sorted(list(all_tags)),
            'estimated_total_time': total_estimated_time,
            'scenarios_root': self.scenarios_root
        }
    
    def save_config(self, file_path: str = None):
        """保存配置到文件"""
        if not file_path:
            file_path = self.config_file or "config/scenarios.yaml"
        
        config = {
            'execution': {
                'scenarios_root': self.scenarios_root,
                'execution_mode': self.execution_mode,
            },
            'filters': self.filters,
            'inter_scenario': self.inter_scenario_config
        }
        
        # 如果是自定义模式，保存自定义顺序
        if self.execution_mode == ScenarioDiscoveryMode.CUSTOM:
            custom_order = []
            for name in self.execution_order:
                scenario = self.scenarios.get(name)
                if scenario:
                    custom_order.append({
                        'name': scenario.name,
                        'directory': os.path.basename(scenario.directory),
                        'enabled': scenario.enabled,
                        'description': scenario.description
                    })
            config['execution']['custom_order'] = custom_order
        
        # 保存配置
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True, indent=2)
        
        self.logger.info(f"Saved configuration to {file_path}")
        
    def backup_config(self) -> str:
        """备份配置文件"""
        if not self.config_file or not Path(self.config_file).exists():
            return ""
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"{self.config_file}.backup.{timestamp}"
        
        import shutil
        shutil.copy2(self.config_file, backup_file)
        
        self.logger.info(f"Backed up configuration to {backup_file}")
        return backup_file