# 配置文件模板

这个目录包含了分布式推理引擎测试系统的配置模板，帮助您快速设置测试环境。

## 模板结构

```
templates/
├── config/                 # 配置文件模板
│   ├── nodes.yaml         # 节点配置模板（脱敏版）
│   ├── scenarios.yaml     # 场景配置模板（完整注释版）
│   └── scenarios/         # 场景模板目录
│       ├── 001xxxx_test/  # 基础场景模板
│       │   ├── metadata.yaml              # 场景元数据模板（详细注释）
│       │   ├── docker-compose-pd.yaml     # Docker Compose配置模板
│       │   ├── test_config.json           # 基准测试配置模板
│       │   └── test_config_commented.jsonc # 带注释的配置说明
│       └── 002_with_dependencies/         # 复杂依赖关系场景模板
│           ├── metadata.yml               # 复杂依赖配置示例
│           └── docker-compose.yml         # 多服务依赖示例
└── README.md              # 本文件
```

## 快速开始

### 1. 初始化配置

```bash
# 从项目根目录执行
cp -r templates/config/* config/
```

### 2. 自定义配置

#### 节点配置 (`config/nodes.yaml`)
- 替换示例IP地址为实际节点IP
- 配置SSH连接信息（用户名、密码或密钥）
- 设置节点角色和工作目录

#### 场景配置 (`config/scenarios.yaml`)
- 配置场景执行顺序
- 设置过滤规则和执行策略
- 调整场景间等待时间和重试配置

#### 场景目录 (`config/scenarios/`)
- 复制场景模板创建新场景
- 修改元数据、Docker配置和测试脚本

## 配置文件说明

### 节点配置 (`../config/nodes.yaml`)

定义分布式节点的连接信息和配置：
- SSH连接参数
- 工作目录配置
- 节点角色和标签
- 启用/禁用状态

### 场景配置 (`../config/scenarios.yaml`)

控制场景的执行顺序和行为：
- 执行模式选择（auto/directory/custom）
- 自定义执行顺序和过滤规则
- 🚀 **并发部署配置**：基于依赖关系的智能并发部署
- 🔄 **智能重试策略**：场景级和服务级重试机制
- 🔒 **资源隔离配置**：确保场景间完全隔离
- 场景间等待和错误处理

### 基准测试配置 (`../config/benchmark.yaml`)

基准测试的默认参数和模板：
- 默认测试参数
- 预定义测试模板
- 性能阈值配置
- 超时和重试设置

## 环境变量

配置文件支持环境变量替换，使用 `${VAR_NAME}` 或 `${VAR_NAME:default_value}` 格式：

```yaml
password: "${NODE_PASSWORD}"
base_url: "${API_URL:http://10.112.0.201:18008}"
```

建议创建 `.env` 文件或设置环境变量：
```bash
export NODE1_PASSWORD="your_password"
export NODE2_PASSWORD="your_password"  
export API_URL="http://your-api-server:8080"
```

## 最佳实践

1. **场景命名**: 使用数字前缀（如 `001_baseline`）来控制执行顺序
2. **元数据完整**: 填写完整的 `metadata.yml` 信息，便于管理和追踪
3. **配置验证**: 使用 playbook 的验证命令检查配置正确性
4. **资源规划**: 在 `metadata.yml` 中明确资源需求
5. **文档更新**: 添加新场景后更新相关文档
6. **🚀 依赖设计**: 合理设计服务依赖关系，利用并发部署优势
7. **🔒 场景隔离**: 利用系统的资源隔离机制，避免场景间相互影响
8. **⚡ 并发调优**: 根据硬件环境调整`max_concurrent_services`等参数
9. **🎯 健康检查**: 配置合适的健康检查策略，确保服务稳定性
10. **📊 监控日志**: 启用详细日志，便于问题排查和性能分析

## 常用场景模式

### 性能基准测试
```json
{
  "num_prompts": 1000,
  "max_concurrency": 20,
  "metadata": {
    "test_type": "baseline"
  }
}
```

### 压力测试
```json
{
  "num_prompts": 5000,
  "max_concurrency": 50,
  "metadata": {
    "test_type": "stress"
  }
}
```

### 快速验证测试
```json
{
  "num_prompts": 100,
  "max_concurrency": 5,
  "metadata": {
    "test_type": "quick"
  }
}
```

## 使用指南

### 创建新场景

```bash
# 复制场景模板创建新场景
cp -r templates/config/scenarios config/scenarios/007_new_scenario

# 修改配置文件
cd config/scenarios/007_new_scenario
vi metadata.yaml
vi docker-compose-pd.yaml
vi run_test.sh
vi test_config.json

# 添加执行权限
chmod +x run_test.sh
```

### 重要提示

1. **配置安全**: `config/` 目录中的文件不会提交到git，请妥善保管实际配置
2. **环境变量**: 使用环境变量保护敏感信息，避免明文密码
3. **模板更新**: 当项目模板更新时，需要手动合并新功能到您的配置中
4. **路径配置**: 确保所有路径配置符合您的实际环境

### 环境变量设置示例

```bash
# 设置节点密码
export NODE1_PASSWORD="your_node1_password"
export NODE2_PASSWORD="your_node2_password"
export NODE3_PASSWORD="your_node3_password"
export NODE4_PASSWORD="your_node4_password"

# 如果启用通知功能
export EMAIL_USERNAME="your_email_username"
export EMAIL_PASSWORD="your_email_password"
export WEBHOOK_URL="https://your-webhook-url.com/notify"
export WEBHOOK_TOKEN="your_webhook_token"
```