"""
配置加载器工具
处理YAML配置文件的加载、验证和环境变量替换
"""

import yaml
import os
import re
from typing import Dict, Any, List, Optional
from pathlib import Path
import logging


class ConfigError(Exception):
    """配置错误"""
    pass


class ConfigLoader:
    """配置加载器"""
    
    def __init__(self):
        self.logger = logging.getLogger("config_loader")
        self.env_pattern = re.compile(r'\$\{([^}]+)\}')
    
    def load_yaml(self, file_path: str, validate_schema: bool = True) -> Dict[str, Any]:
        """加载YAML配置文件"""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise ConfigError(f"Configuration file not found: {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 替换环境变量
            content = self._replace_env_vars(content)
            
            # 解析YAML
            config = yaml.safe_load(content)
            
            if config is None:
                config = {}
            
            self.logger.info(f"Loaded configuration from {file_path}")
            return config
            
        except yaml.YAMLError as e:
            raise ConfigError(f"Invalid YAML syntax in {file_path}: {e}")
        except Exception as e:
            raise ConfigError(f"Failed to load configuration from {file_path}: {e}")
    
    def _replace_env_vars(self, content: str) -> str:
        """替换环境变量"""
        def replace_match(match):
            var_expr = match.group(1)
            # 支持默认值语法: ${VAR:default_value}
            if ':' in var_expr:
                var_name, default_value = var_expr.split(':', 1)
                return os.getenv(var_name.strip(), default_value.strip())
            else:
                var_name = var_expr.strip()
                value = os.getenv(var_name)
                if value is None:
                    self.logger.warning(f"Environment variable {var_name} not found")
                    return match.group(0)  # 保持原样
                return value
        
        return self.env_pattern.sub(replace_match, content)
    
    def validate_required_fields(self, config: Dict[str, Any], 
                                required_fields: List[str], 
                                section_name: str = "config") -> bool:
        """验证必需字段"""
        missing_fields = []
        
        for field in required_fields:
            if '.' in field:
                # 嵌套字段检查
                parts = field.split('.')
                current = config
                try:
                    for part in parts:
                        current = current[part]
                except (KeyError, TypeError):
                    missing_fields.append(field)
            else:
                if field not in config:
                    missing_fields.append(field)
        
        if missing_fields:
            raise ConfigError(
                f"Missing required fields in {section_name}: {missing_fields}"
            )
        
        return True
    
    def merge_configs(self, base_config: Dict[str, Any], 
                     override_config: Dict[str, Any]) -> Dict[str, Any]:
        """合并配置文件"""
        def deep_merge(base: Dict, override: Dict) -> Dict:
            result = base.copy()
            
            for key, value in override.items():
                if (key in result and 
                    isinstance(result[key], dict) and 
                    isinstance(value, dict)):
                    result[key] = deep_merge(result[key], value)
                else:
                    result[key] = value
            
            return result
        
        return deep_merge(base_config, override_config)
    
    def expand_paths(self, config: Dict[str, Any], base_path: str = None) -> Dict[str, Any]:
        """展开相对路径"""
        if base_path:
            base_path = Path(base_path).parent
        else:
            base_path = Path.cwd()
        
        def expand_value(value):
            if isinstance(value, str) and value.startswith('./'):
                return str((base_path / value[2:]).resolve())
            elif isinstance(value, dict):
                return {k: expand_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [expand_value(item) for item in value]
            else:
                return value
        
        return expand_value(config)


def load_config(file_path: str, **kwargs) -> Dict[str, Any]:
    """便利函数：加载配置文件"""
    loader = ConfigLoader()
    return loader.load_yaml(file_path, **kwargs)


def validate_config(config: Dict[str, Any], required_fields: List[str], 
                   section_name: str = "config") -> bool:
    """便利函数：验证配置"""
    loader = ConfigLoader()
    return loader.validate_required_fields(config, required_fields, section_name)