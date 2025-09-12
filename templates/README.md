# 配置文件模板

这个目录包含了各种配置文件和场景的模板，帮助您快速创建新的测试配置。

## 模板列表

### 1. 场景模板 (`scenario_template/`)

创建新的测试场景时，可以复制这个模板目录并根据需求进行修改。

**包含文件：**
- `metadata.yml` - 场景元数据配置
- `docker-compose.yml` - Docker Compose服务配置
- `run_test.sh` - 测试执行脚本

**使用方法：**
```bash
# 复制模板创建新场景
cp -r templates/scenario_template scenarios/007_new_scenario

# 修改配置文件
cd scenarios/007_new_scenario
vi metadata.yml
vi docker-compose.yml
vi run_test.sh

# 添加执行权限
chmod +x run_test.sh
```

## 配置文件说明

### 节点配置 (`../config/nodes.yaml`)

定义分布式节点的连接信息和配置：
- SSH连接参数
- 工作目录配置
- 节点角色和标签
- 启用/禁用状态

### 场景配置 (`../config/scenarios.yaml`)

控制场景的执行顺序和行为：
- 执行模式选择
- 自定义执行顺序
- 过滤规则配置
- 场景间等待和重试

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