# 分布式推理引擎自动化测试Playbook

这是一个专门为分布式推理引擎设计的自动化测试系统，能够在多个节点上按顺序执行不同的测试场景，收集性能数据，并生成详细的测试报告。

## 🚀 核心功能

- **分布式节点管理**: 通过SSH管理多个测试节点，支持连接池和自动重连
- **场景化测试**: 支持完全可配置的测试场景和执行顺序，三种执行模式
- **Docker服务管理**: 远程管理推理服务的启动和停止，支持Docker Compose版本自适应
- **基准测试执行**: 集成AICP基准测试，支持多种测试配置和并行执行
- **结果自动收集**: 从各节点收集测试数据并按场景归档，支持多种输出格式
- **健康状态监控**: 全面的系统健康检查和容错机制，支持自动恢复
- **丰富的CLI工具**: 命令行界面方便操作和监控，支持详细模式和干运行
- **智能版本适配**: 自动检测Docker Compose版本(V1/V2)并适配相应命令
- **强化错误处理**: 完善的异常处理和重试机制，提高系统稳定性

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
│   │   └── scenarios/      # 场景模板目录
│   └── README.md           # 模板使用说明
├── src/                     # 源代码
│   ├── playbook/           # 核心模块
│   │   ├── core.py         # 核心控制器
│   │   ├── node_manager.py # 节点管理
│   │   ├── scenario_*.py   # 场景管理和执行
│   │   ├── docker_*.py     # Docker服务管理
│   │   ├── benchmark_runner.py # 基准测试执行
│   │   ├── result_collector.py # 结果收集
│   │   └── health_checker.py   # 健康检查
│   └── utils/              # 工具模块
│       ├── ssh_client.py   # SSH连接工具
│       ├── config_loader.py # 配置加载器
│       ├── logger.py       # 日志工具
│       └── docker_compose_adapter.py # Docker Compose版本适配
├── logs/                    # 日志文件
└── results/                # 测试结果
```

## 🔧 安装和配置

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

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
```

### 3. 设置环境变量

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

## 📊 测试结果

测试完成后，结果会按照日期和场景进行组织：

```
results/
├── 20240906_143022/
│   ├── baseline_test/
│   │   ├── node1/
│   │   ├── node2/
│   │   ├── test_metadata.json
│   │   ├── result_summary.json
│   │   ├── performance_results.csv
│   │   └── test_report.md
```

每个场景的结果包含：
- 原始测试数据文件
- 性能指标汇总 
- 测试元数据
- CSV格式的性能数据
- Markdown测试报告

## 🔄 执行流程

1. **系统健康检查**: 验证所有节点和服务状态
2. **环境准备**: 上传配置文件，准备测试环境
3. **服务启动**: 在各节点启动推理服务
4. **等待就绪**: 等待服务完全启动和健康检查通过
5. **执行测试**: 运行AICP基准测试
6. **收集结果**: 从各节点下载测试结果
7. **服务停止**: 停止推理服务
8. **环境清理**: 清理临时文件和资源
9. **生成报告**: 汇总数据并生成测试报告

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

### 并行执行和性能优化

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

1. **场景命名**: 使用数字前缀控制执行顺序（如 `001_baseline`）
2. **配置管理**: 使用环境变量管理敏感信息
3. **资源规划**: 在metadata.yaml中明确资源需求
4. **错误处理**: 启用continue_on_failure进行批量测试
5. **结果管理**: 定期清理旧的测试结果，避免磁盘空间不足
6. **监控日志**: 使用详细日志模式进行问题排查

## 🐛 故障排查

### 常见问题

1. **SSH连接失败**: 检查节点配置和网络连通性
2. **Docker服务启动失败**: 验证docker-compose文件和镜像
3. **测试执行超时**: 调整超时配置或检查资源使用情况
4. **结果收集失败**: 检查结果路径和权限设置

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