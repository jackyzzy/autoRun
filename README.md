# 分布式推理引擎自动化测试Playbook

这是一个专门为分布式推理引擎设计的自动化测试系统，能够在多个节点上按顺序执行不同的测试场景，收集性能数据，并生成详细的测试报告。

## 🚀 核心功能

- **分布式节点管理**: 通过SSH管理多个测试节点，支持连接池和自动重连
- **场景化测试**: 支持完全可配置的测试场景和执行顺序，三种执行模式
- **🔒 资源完全隔离**: Scenario间完全资源隔离，确保每个测试在干净环境中运行
- **Docker服务管理**: 远程管理推理服务的启动和停止，支持Docker Compose版本自适应
- **基准测试执行**: 集成AICP基准测试，支持多种测试配置和并行执行
- **💾 智能结果收集**: 支持三种收集模式(basic/standard/comprehensive)，根据场景复杂度灵活配置
- **健康状态监控**: 全面的系统健康检查和容错机制，支持自动恢复
- **丰富的CLI工具**: 命令行界面方便操作和监控，支持详细模式和干运行
- **智能版本适配**: 自动检测Docker Compose版本(V1/V2)并适配相应命令
- **强化错误处理**: 完善的异常处理和重试机制，提高系统稳定性
- **⚡ 并发部署优化**: 基于依赖关系的智能并发部署，大幅提升部署效率

## 📁 项目结构

```
test_playbook/
├── playbook.py              # 主程序入口
├── requirements.txt         # Python依赖
├── config/                  # 配置文件（实际使用的配置，不提交到git）
│   ├── nodes.yaml          # 节点配置（从模板复制并自定义）
│   ├── scenarios.yaml      # 场景配置（从模板复制并自定义）
│   └── scenarios/          # 测试场景目录
│       ├── 001_baseline/   # 基线测试场景
│       ├── 002_memory_opt/ # 内存优化场景
│       └── ...             # 更多场景
├── templates/               # 配置模板和示例
│   ├── config/             # 配置文件模板
│   │   ├── nodes.yaml      # 脱敏的节点配置模板
│   │   ├── scenarios.yaml  # 脱敏的场景配置模板
│   │   ├── defaults.yaml   # 全局默认配置模板
│   │   └── scenarios/      # 场景模板目录
│   └── README.md           # 模板使用说明
├── src/                     # 源代码
│   ├── playbook/           # 核心模块
│   │   ├── core.py         # 核心控制器
│   │   ├── node_manager.py # 节点管理
│   │   ├── scenario_*.py   # 场景管理和执行
│   │   ├── scenario_resource_manager.py # 资源隔离管理器
│   │   ├── docker_*.py     # Docker服务管理
│   │   ├── concurrent_deployer.py # 并发部署管理器
│   │   ├── health_check_manager.py # 健康检查管理器
│   │   ├── benchmark_runner.py # 基准测试执行
│   │   ├── result_collector.py # 结果收集
│   │   ├── test_script_executor.py # 测试脚本执行器
│   │   └── exceptions.py   # 异常定义
│   └── utils/              # 工具模块
│       ├── ssh_client.py   # SSH连接工具（SCP传输优化）
│       ├── config_loader.py # 配置加载器
│       ├── config_validator.py # 配置验证器
│       ├── global_config_manager.py # 全局配置管理器
│       ├── logger.py       # 日志工具
│       ├── common.py       # 公共工具函数
│       └── docker_compose_adapter.py # Docker Compose版本适配
├── logs/                    # 日志文件
└── results/                # 测试结果
```

## 🔧 安装和配置

### 1. 安装依赖

#### 本地安装
```bash
pip install -r requirements.txt
```

#### Docker容器部署
```bash
# 构建镜像
docker build -t test-playbook .

# 运行容器
docker run -it --rm \
  -v $(pwd)/config:/workspace/playbook/config \
  -v $(pwd)/results:/workspace/playbook/results \
  -e NODE1_PASSWORD="your_password" \
  test-playbook status
```

**主要依赖包说明：**
- `paramiko`: SSH连接和文件传输
- `scp`: SCP文件传输优化（相比SFTP更高效且避免验证阻塞）
- `click`: 命令行界面
- `rich`: 美化输出和进度显示
- `PyYAML`: 配置文件解析

### 2. 配置文件设置

**重要提示**: `config/` 目录中的文件不会提交到git仓库，这些是您的实际配置文件。

#### 初始化配置文件

从模板复制配置文件：

```bash
# 复制配置模板
cp -r templates/config/* config/

# 或者单独复制需要的文件
cp templates/config/nodes.yaml config/
cp templates/config/scenarios.yaml config/
cp -r templates/config/scenarios config/
```

#### 配置节点信息

编辑 `config/nodes.yaml`，替换为您的实际配置：

```yaml
nodes:
  node1:
    host: "YOUR_ACTUAL_IP"              # 替换为实际IP地址
    username: "root"                    # 替换为实际用户名
    password: "${NODE1_PASSWORD}"       # 设置环境变量
    enabled: true
    docker_compose_path: "/opt/inference"
    results_path: "/opt/benchmark/results"
```

#### 配置场景执行

编辑 `config/scenarios.yaml`：

```yaml
execution:
  scenarios_root: "config/scenarios"   # 场景根目录
  execution_mode: "custom"
  custom_order:
    - name: "baseline_test"
      directory: "001_baseline"
      enabled: true
      description: "基线性能测试"

execution_config:
  # 结果收集默认模式
  default_collection_mode: "standard"

  # 并发部署配置
  concurrent_deployment:
    max_concurrent_services: 5
    max_concurrent_health_checks: 10
    deployment_timeout: 600
    health_check_timeout: 300
```

### 3. 三层配置系统

Playbook 采用三层配置系统，配置优先级为：**场景配置 > 全局配置 > 系统默认值**

#### 配置层级说明

1. **场景配置**（最高优先级）
   - 位置：`config/scenarios/{scenario_name}/metadata.yaml`
   - 作用：特定场景的专用配置

2. **全局配置**（中等优先级）
   - 位置：`config/defaults.yaml`
   - 作用：项目级别的默认配置

3. **系统默认值**（最低优先级）
   - 位置：代码内置
   - 作用：保证系统基本运行的后备配置

#### 配置全局默认值

复制并编辑全局配置文件：

```bash
# 复制全局配置模板
cp templates/config/defaults.yaml config/

# 编辑全局配置
vi config/defaults.yaml
```

**全局配置示例：**
```yaml
# 服务健康检查配置
service_health_check:
  enabled: true
  strategy: "standard"  # quick | standard | thorough
  startup_timeout: 200  # 服务启动超时（秒）
  max_retries: 4        # 最大重试次数

# 测试执行配置
test_execution:
  timeout: 2400         # 测试超时时间（秒）
  node: "local"         # 执行节点: local | remote | auto

# 并发执行配置
concurrent_execution:
  max_concurrent_services: 4        # 最大并发服务数
  deployment_timeout: 450           # 部署超时（秒）
  max_concurrent_health_checks: 7   # 最大并发健康检查数
```

### 4. 设置环境变量

```bash
export NODE1_PASSWORD="your_actual_password"
export NODE2_PASSWORD="your_actual_password"
export NODE3_PASSWORD="your_actual_password"
```

## 🎯 使用方法

### 查看系统状态

```bash
./playbook.py status
```

### 列出所有场景

```bash
./playbook.py scenarios
```

### 列出所有节点

```bash
./playbook.py nodes
```

### 运行健康检查

```bash
./playbook.py health
```

### 运行单个场景

```bash
./playbook.py run baseline_test
```

### 运行所有场景

```bash
./playbook.py run --all
```

### 验证配置

```bash
./playbook.py validate
```

### 场景管理

```bash
# 启用场景
./playbook.py scenario baseline_test --enable

# 禁用场景
./playbook.py scenario baseline_test --disable

# 查看场景详情
./playbook.py scenario baseline_test
```

### 查看测试结果

```bash
./playbook.py results
```

## 📝 场景配置

每个测试场景包含以下文件：

### metadata.yaml - 场景元数据
```yaml
name: "baseline_test"
description: "基线性能测试"
estimated_duration: 1800
tags: ["performance", "baseline"]
resource_requirements:
  min_gpu_memory: "24GB"
  min_nodes: 2

# 测试执行配置
test_execution:
  node: "local"
  script: "run_test.sh"
  timeout: 2400
  result_paths: ["results/"]
  # 结果收集模式 (basic/standard/comprehensive)
  collection_mode: "standard"
```

### docker-compose.yml - 服务配置
```yaml
services:
  p-1:
    image: "inference-engine:latest"
    ports:
      - "18008:8000"
  d-1:
    image: "data-processor:latest"
    ports:
      - "18009:8001"
```

### test_config.json - 测试参数
```json
{
  "base_url": "http://10.112.0.201:18008",
  "model": "/data/Qwen3-235B-A22B",
  "num_prompts": 1000,
  "max_concurrency": 20,
  "metadata": {
    "scenario": "baseline_test",
    "gpu_num": 4
  }
}
```

### 测试脚本环境变量

测试脚本执行时，系统会自动设置以下环境变量：

| 环境变量 | 说明 | 示例值 | 执行环境 |
|---------|------|--------|----------|
| `SCENARIO_NAME` | 当前执行的场景名称 | `baseline_test` | 本地/远程 |
| `SCENARIO_PATH` | 场景工作目录路径 | `/opt/inference` | 远程 |
| `SCENARIO_RESULT_PATH` | 测试结果存储路径 | `/opt/benchmark/results` | **本地/远程** |

**重要更新**: `SCENARIO_RESULT_PATH` 现在在本地执行和远程执行中都可用，支持多路径配置（使用路径分隔符连接）。

**在测试脚本中使用环境变量：**

```bash
#!/bin/bash
# run_test.sh

# 使用系统提供的环境变量
echo "执行场景: $SCENARIO_NAME"
echo "工作目录: $SCENARIO_PATH"
echo "结果路径: $SCENARIO_RESULT_PATH"

# 确保结果目录存在
mkdir -p "$SCENARIO_RESULT_PATH"

# 运行测试并将结果保存到指定路径
docker run --rm \
  -v "$SCENARIO_RESULT_PATH:/benchmark/results" \
  your-test-image \
  --scenario "$SCENARIO_NAME" \
  --output-dir "/benchmark/results"
```

## 📊 测试结果

测试完成后，结果会按照日期和场景进行组织：

```
results/
├── 20240906_143022/
│   ├── baseline_test/
│   │   ├── test_artifacts/          # 测试产物和结果文件
│   │   ├── nodes/                   # 节点相关数据
│   │   │   ├── node1/
│   │   │   │   ├── services/        # 服务日志
│   │   │   │   └── system/          # 系统日志（comprehensive模式）
│   │   │   └── node2/
│   │   ├── test_metadata.json
│   │   ├── result_summary.json
│   │   ├── performance_results.csv
│   │   └── test_report.md
```

### 结果收集模式

系统支持三种结果收集模式，可在场景配置中指定：

- **basic**: 仅收集测试结果文件，适用于简单测试
- **standard**: 收集测试结果 + 服务日志，适用于大部分分布式测试
- **comprehensive**: 收集测试结果 + 服务日志 + 系统日志，适用于复杂系统诊断

每个场景的结果包含：
- 原始测试数据文件和产物
- 性能指标汇总和详细数据
- 测试元数据和执行日志
- CSV格式的性能数据
- Markdown测试报告
- 服务和系统日志（根据收集模式）

## 🔒 资源完全隔离系统

Playbook 实现了场景间的完全资源隔离，确保每个测试场景在完全干净的环境中运行，避免状态污染和连接冲突。

### 核心特性

- **🧹 完全资源清理**: 每个scenario执行完毕后，自动清理所有SSH连接、临时文件和内存状态
- **🔍 清理验证机制**: 验证环境确实已清理干净，确保下个scenario在纯净环境中启动
- **⚡ 紧急恢复**: 当常规清理失败时，自动启动紧急清理机制
- **📊 详细统计**: 追踪清理成功率、错误类型和性能指标

### 隔离机制

1. **SSH连接池清理**: 强制关闭所有SSH连接和SFTP会话
2. **临时文件清理**: 清理系统临时目录中的playbook相关文件
3. **内存状态重置**: 强制垃圾回收，清理内存中的缓存状态
4. **环境验证**: 验证清理效果，确保环境完全干净

### 配置示例

系统会在每个scenario之间自动执行资源清理：

```python
# 自动执行的清理步骤
cleanup_success = resource_manager.cleanup_scenario_resources(scenario_name)
if not cleanup_success:
    logger.warning("Resource cleanup failed, but continuing...")
```

### 监控和统计

可以查看资源清理的统计信息：

```bash
# 在日志中查看详细的清理过程
grep "🧹\|✅\|❌" logs/playbook_*.log
```

### 故障恢复

- **自动重试**: 清理失败时自动重试
- **降级清理**: 常规清理失败时启动紧急清理模式
- **错误记录**: 详细记录所有清理错误供分析

## 🔄 执行流程

### 单个Scenario执行流程

1. **系统健康检查**: 验证所有节点和服务状态
2. **环境准备**: 上传配置文件，准备测试环境
3. **🚀 并发服务部署**: 基于依赖关系智能批次化部署服务
4. **等待就绪**: 等待服务完全启动和健康检查通过
5. **执行测试**: 运行AICP基准测试
6. **收集结果**: 从各节点下载测试结果
7. **服务停止**: 停止推理服务
8. **环境清理**: 清理临时文件和资源

### 多Scenario执行流程

当执行 `--all` 时，系统会在scenarios之间进行完全资源隔离：

1. **初始健康检查**: 验证系统整体状态
2. **Scenario A执行**: 执行第一个scenario的完整流程
3. **🧹 完全资源清理**: 清理SSH连接池、临时文件、内存状态
4. **🔍 环境验证**: 验证环境已完全清理干净
5. **Scenario B执行**: 在纯净环境中执行下一个scenario
6. **重复步骤3-5**: 直到所有scenarios执行完毕
7. **最终清理**: 执行最终的资源清理
8. **生成报告**: 汇总所有数据并生成测试报告

## 🛠️ 高级功能

### 场景管理模式

支持三种场景发现模式：
- **auto**: 自动发现并按目录名排序
- **directory**: 严格按目录名排序执行  
- **custom**: 完全自定义执行顺序

### 过滤和条件执行

```yaml
filters:
  include_tags: ["performance"]
  exclude_tags: ["experimental"]
  only_scenarios: ["baseline_test"]
  skip_scenarios: ["large_model_test"]
```

### 容错和重试

```yaml
inter_scenario:
  wait_between_scenarios: 120
  continue_on_failure: true
  retry_count: 1
```

### Docker Compose版本自适应

系统自动检测节点上的Docker Compose版本(V1或V2)并适配相应的命令格式：
```bash
# 自动检测并使用相应版本
# V1: docker-compose -f file.yml up -d
# V2: docker compose -f file.yml up -d
```

### 🚀 并发部署系统

Playbook 支持基于依赖关系的智能并发部署，显著减少总体部署时间：

#### 核心特性

- **🎯 批次化部署**: 基于服务依赖关系自动分批，确保依赖顺序正确
- **⚡ 批次内并发**: 同一批次内的服务可以并发部署，提高部署效率
- **🔄 智能重试**: 只重试失败的服务，不影响已成功的服务
- **📊 实时监控**: 提供详细的部署进度和状态跟踪

#### 依赖关系处理

系统使用拓扑排序（Kahn算法）自动计算部署批次：

```yaml
# metadata.yml 中定义服务依赖
deployment:
  services:
    # Batch 1: 基础服务（无依赖）
    - name: "redis"
      depends_on: []
    - name: "mysql"
      depends_on: []

    # Batch 2: 应用服务（依赖基础服务）
    - name: "user-service"
      depends_on: ["redis", "mysql"]
    - name: "order-service"
      depends_on: ["redis", "mysql"]

    # Batch 3: 网关服务（依赖应用服务）
    - name: "api-gateway"
      depends_on: ["user-service", "order-service"]
```

#### 并发配置

在 `config/scenarios.yaml` 中配置并发行为：

```yaml
execution_config:
  concurrent_deployment:
    # 批次内最大并发服务数（建议3-8）
    max_concurrent_services: 5

    # 健康检查最大并发数（建议5-15）
    max_concurrent_health_checks: 10

    # 部署超时时间（秒，建议300-900）
    deployment_timeout: 600

    # 健康检查超时时间（秒，建议120-600）
    health_check_timeout: 300

  retry_strategy:
    # 服务级重试次数（建议1-3）
    service_level_retries: 2

    # 重试间隔时间（秒，建议15-60）
    retry_delay: 30

    # 只重试失败的服务（推荐开启）
    retry_only_failed: true
```

#### 性能优势

与传统串行部署相比：

- **🚀 部署时间**: 节省30%-70%的总部署时间
- **⚡ 并发效率**: 批次内服务并发启动，充分利用系统资源
- **🔧 智能重试**: 只重试失败服务，节省50%-80%重试时间
- **📈 扩展性**: 可配置并发度，适应不同硬件环境

#### 工作原理

```
传统串行部署：
Service A → Service B → Service C → Service D
总时间 = A + B + C + D = 20分钟

并发批次部署：
Batch 1: [Service A, Service B] (并发) → 8分钟
Batch 2: [Service C, Service D] (并发) → 6分钟
总时间 = max(A,B) + max(C,D) = 14分钟 (节省30%)
```

#### 调优建议

**高性能环境** (充足CPU/内存):
```yaml
max_concurrent_services: 8-12
max_concurrent_health_checks: 15-20
```

**普通环境** (平衡性能和稳定性):
```yaml
max_concurrent_services: 3-6
max_concurrent_health_checks: 8-12
```

**受限环境** (避免资源竞争):
```yaml
max_concurrent_services: 1-2
max_concurrent_health_checks: 3-5
```

### 其他性能优化特性

- **连接池管理**: 智能SSH连接复用，减少连接开销
- **并行健康检查**: 同时检查多个节点状态
- **智能重试**: 基于错误类型的差异化重试策略
- **缓存机制**: 缓存版本检测和配置验证结果

### 自动恢复

健康检查器支持自动恢复机制，可以在检测到问题时自动尝试修复：
- Docker服务自动重启
- SSH连接自动重连
- 失败节点的隔离和恢复

## 📋 最佳实践

### 1. 场景管理
- **命名规范**: 使用数字前缀控制执行顺序（如 `001_baseline`）
- **目录结构**: 保持场景目录结构的一致性
- **资源规划**: 在metadata.yaml中明确资源需求

### 2. 配置管理
- **环境变量**: 使用环境变量管理敏感信息
- **三层配置**: 充分利用三层配置系统的优势
  - 场景特定配置放在 `metadata.yaml`
  - 项目级配置放在 `config/defaults.yaml`
  - 让系统默认值处理基础配置
- **配置验证**: 使用 `./playbook.py validate` 验证配置文件

### 3. 测试脚本编写
- **环境变量使用**: 优先使用系统提供的环境变量
  ```bash
  # 推荐：使用环境变量
  mkdir -p "$SCENARIO_RESULT_PATH"

  # 不推荐：硬编码路径
  mkdir -p "/opt/benchmark/results"
  ```
- **错误处理**: 在脚本开始添加 `set -e` 确保错误时停止
- **日志记录**: 使用有意义的日志输出，便于调试

### 4. 性能优化
- **并发配置**: 根据硬件资源调整 `max_concurrent_services`
- **传输方式**: 利用SCP传输的性能优势（自动启用）
- **超时设置**: 根据实际测试复杂度调整超时时间

### 5. 运维管理
- **错误处理**: 启用continue_on_failure进行批量测试
- **结果管理**: 定期清理旧的测试结果，避免磁盘空间不足
- **监控日志**: 使用详细日志模式进行问题排查
- **健康检查**: 定期运行 `./playbook.py health` 检查系统状态

## 🐛 故障排查

### 常见问题

1. **SSH连接失败**: 检查节点配置和网络连通性
2. **Docker服务启动失败**: 验证docker-compose文件和镜像
3. **测试执行超时**: 调整超时配置或检查资源使用情况
4. **结果收集失败**: 检查结果路径和权限设置
5. **"Garbage packet received"错误**: 已通过资源隔离系统解决，如仍出现请检查网络环境
6. **Scenario间状态污染**: 系统已实现完全隔离，每个scenario在独立环境中运行
7. **资源清理失败**: 系统会自动启动紧急清理，查看日志了解详情

### 调试命令

```bash
# 详细模式运行
./playbook.py --verbose --log-level DEBUG status

# 验证配置
./playbook.py validate

# 健康检查
./playbook.py health

# 干运行 - 验证配置但不执行
./playbook.py run --dry-run

# 运行单个场景并详细输出
./playbook.py --verbose run baseline_test

# 查看结果的不同格式
./playbook.py results --format json
./playbook.py results --format yaml
./playbook.py results --format table
```

## 📈 性能优化建议

1. **并发控制**: 根据硬件资源调整max_concurrency
2. **批处理**: 使用合适的batch_size提高吞吐量
3. **内存管理**: 监控GPU和系统内存使用情况
4. **网络优化**: 确保节点间网络带宽充足
5. **存储IO**: 使用高性能存储存放模型和结果数据

## 🤝 贡献指南

1. Fork本项目
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启Pull Request

## 📄 许可证

本项目采用MIT许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 📞 支持

如果您在使用过程中遇到问题：

1. 查看 [FAQ](docs/FAQ.md)
2. 搜索已有的 [Issues](https://github.com/your-org/test_playbook/issues)
3. 创建新的 Issue 描述您的问题
4. 联系开发团队获取支持

---

🚀 **Happy Testing!** 🚀