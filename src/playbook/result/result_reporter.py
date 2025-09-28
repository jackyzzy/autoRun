"""
结果报告生成器
负责生成各种格式的测试结果报告
"""

import json
import yaml
import tarfile
import logging
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from datetime import datetime

from .result_models import CollectionSummary, ResultSummary, TestSuiteResultSummary


class ResultReporter:
    """结果报告生成器"""
    
    def __init__(self):
        self.logger = logging.getLogger("playbook.result_reporter")
    
    def save_result_summary(self, result_dir: Path, summary: ResultSummary):
        """保存结果摘要"""
        summary_file = result_dir / "result_summary.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary.to_dict(), f, indent=2, ensure_ascii=False)
        
        # 同时保存YAML格式
        yaml_file = result_dir / "result_summary.yaml"
        with open(yaml_file, 'w', encoding='utf-8') as f:
            yaml.dump(summary.to_dict(), f, default_flow_style=False, allow_unicode=True)
        
        self.logger.debug(f"Saved result summary to {summary_file}")
    
    def generate_reports_v2(self, result_dir: Path, summary: ResultSummary,
                           collection_summary: CollectionSummary):
        """生成报告 - 新版本"""
        try:
            # 生成Markdown报告
            self.generate_markdown_report_v2(result_dir, summary, collection_summary)
            
            # 生成收集统计报告
            self.generate_collection_report(result_dir, collection_summary)
            
            # 生成性能分析报告
            self.generate_performance_report(result_dir, summary)

        except Exception as e:
            self.logger.warning(f"Failed to generate some reports: {e}")

    def generate_markdown_report_v2(self, result_dir: Path, summary: ResultSummary,
                                    collection_summary: CollectionSummary):
        """生成Markdown报告 - 新版本"""
        try:
            def safe_format(value, format_spec=''):
                if value is None:
                    return 'N/A'
                if format_spec:
                    return f"{value:{format_spec}}"
                return str(value)

            report_content = f"""# 测试结果报告 - {summary.scenario_name}

## 基本信息
- **场景名称**: {summary.scenario_name}
- **测试时间**: {summary.timestamp}
- **收集模式**: {collection_summary.mode}
- **总节点数**: {summary.total_nodes}
- **成功节点数**: {summary.successful_nodes}
- **失败节点数**: {summary.failed_nodes}

## 测试结果收集
- **测试artifacts**: {collection_summary.test_artifacts.get('count', 0)} 个文件
- **artifacts大小**: {safe_format(collection_summary.test_artifacts.get('total_size_mb', 0), '.2f')} MB
- **总文件数**: {collection_summary.total_files_collected}
- **总大小**: {safe_format(collection_summary.total_size_mb, '.2f')} MB

## 分布式结果收集

"""

            # 每个节点的收集结果
            if collection_summary.node_results:
                report_content += "### 节点收集结果\\n\\n"
                report_content += "| 节点名称 | 服务日志 | 系统日志 | 错误数 |\\n"
                report_content += "|----------|----------|----------|----------|\\n"

                for node_name, node_info in collection_summary.node_results.items():
                    services = ', '.join(node_info.get('services_collected', []))
                    system_logs = '✓' if node_info.get('system_logs_collected', False) else '✗'
                    error_count = len(node_info.get('collection_errors', []))
                    report_content += f"| {node_name} | {services} | {system_logs} | {error_count} |\\n"

            # 性能指标（如果有）
            if summary.total_requests > 0:
                report_content += f"""
## 性能指标
- **平均吞吐量**: {safe_format(summary.avg_throughput, '.2f')} tokens/s
- **最大吞吐量**: {safe_format(summary.max_throughput, '.2f')} tokens/s
- **最小吞吐量**: {safe_format(summary.min_throughput, '.2f')} tokens/s
- **平均延迟**: {safe_format(summary.avg_latency, '.3f')} s
- **总请求数**: {summary.total_requests}
- **成功请求数**: {summary.total_successful_requests}
- **总体成功率**: {safe_format(summary.overall_success_rate, '.2f')}%
"""

            # 错误信息
            if collection_summary.collection_errors:
                report_content += "\\n## 收集错误\\n\\n"
                for error in collection_summary.collection_errors:
                    report_content += f"- {error}\\n"

            markdown_file = result_dir / "scenario_test_report.md"
            with open(markdown_file, 'w', encoding='utf-8') as f:
                f.write(report_content)

            self.logger.debug(f"Generated Markdown report: {markdown_file}")

        except Exception as e:
            self.logger.error(f"Failed to generate Markdown report: {e}")

    def generate_collection_report(self, result_dir: Path, collection_summary: CollectionSummary):
        """生成收集统计报告"""
        try:
            report_content = f"""收集时间: {collection_summary.collection_time}
收集模式: {collection_summary.mode}
总文件数: {collection_summary.total_files_collected}
总大小: {collection_summary.total_size_mb:.2f} MB
错误数: {len(collection_summary.collection_errors)}

测试artifacts:
- 数量: {collection_summary.test_artifacts.get('count', 0)}
- 大小: {collection_summary.test_artifacts.get('total_size_mb', 0):.2f} MB
- 来源: {collection_summary.test_artifacts.get('source', 'unknown')}

节点结果:
"""

            for node_name, node_info in collection_summary.node_results.items():
                report_content += f"\\n{node_name}:"
                report_content += f"\\n  服务日志: {', '.join(node_info.get('services_collected', []))}"
                report_content += f"\\n  系统日志: {'Yes' if node_info.get('system_logs_collected', False) else 'No'}"
                if node_info.get('collection_errors'):
                    report_content += f"\\n  错误: {'; '.join(node_info['collection_errors'])}"

            if collection_summary.collection_errors:
                report_content += "\\n\\n全局错误:\\n"
                for error in collection_summary.collection_errors:
                    report_content += f"- {error}\\n"

            collection_report_file = result_dir / "collection_report.txt"
            with open(collection_report_file, 'w', encoding='utf-8') as f:
                f.write(report_content)

            self.logger.debug(f"Generated collection report: {collection_report_file}")

        except Exception as e:
            self.logger.error(f"Failed to generate collection report: {e}")
    
    def generate_performance_report(self, result_dir: Path, summary: ResultSummary):
        """生成性能分析报告"""
        try:
            if summary.total_requests == 0:
                self.logger.debug("No performance data available, skipping performance report")
                return
            
            performance_data = {
                'summary': {
                    'scenario_name': summary.scenario_name,
                    'timestamp': summary.timestamp,
                    'total_nodes': summary.total_nodes,
                    'successful_nodes': summary.successful_nodes,
                    'failed_nodes': summary.failed_nodes
                },
                'metrics': {
                    'avg_throughput': summary.avg_throughput,
                    'max_throughput': summary.max_throughput,
                    'min_throughput': summary.min_throughput,
                    'avg_latency': summary.avg_latency,
                    'total_requests': summary.total_requests,
                    'total_successful_requests': summary.total_successful_requests,
                    'overall_success_rate': summary.overall_success_rate
                },
                'node_details': summary.node_results
            }
            
            # 保存JSON格式的性能数据
            performance_file = result_dir / "performance_report.json"
            with open(performance_file, 'w', encoding='utf-8') as f:
                json.dump(performance_data, f, indent=2, ensure_ascii=False)
            
            self.logger.debug(f"Generated performance report: {performance_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to generate performance report: {e}")

    def generate_suite_report(self, result_dir: Path, summary: Union[ResultSummary, TestSuiteResultSummary]):
        """生成测试套件报告 - 用于汇总报告"""
        try:
            def safe_format(value, format_spec=''):
                if value is None:
                    return 'N/A'
                if format_spec:
                    return f"{value:{format_spec}}"
                return str(value)

            # 检查summary类型并生成相应的报告
            if isinstance(summary, TestSuiteResultSummary):
                # 生成测试套件汇总报告
                report_content = f"""# 测试套件汇总报告

## 基本信息
- **套件名称**: {summary.suite_name}
- **测试时间**: {summary.timestamp}
- **总场景数**: {summary.total_scenarios}
- **成功场景数**: {summary.successful_scenarios}
- **失败场景数**: {summary.failed_scenarios}
- **总体成功率**: {safe_format(summary.overall_success_rate_pct, '.1f')}%
- **总执行时间**: {safe_format(summary.total_execution_time, '.1f')} 秒

## 性能指标汇总
- **总体平均吞吐量**: {safe_format(summary.overall_avg_throughput, '.2f')} tokens/s
- **总体最大吞吐量**: {safe_format(summary.overall_max_throughput, '.2f')} tokens/s
- **总体成功率**: {safe_format(summary.overall_success_rate, '.1f')}%

## 文件统计汇总
- **总结果文件数**: {summary.total_result_files}
- **总文件大小**: {safe_format(summary.total_size_mb, '.2f')} MB

## 各场景详细结果

| 场景名称 | 状态 | 执行时长(秒) | 结果文件数 | 文件大小(MB) | 成功节点 | 失败节点 | 错误信息 |
|---------|------|-------------|------------|-------------|----------|----------|----------|
"""

                for scenario_name, scenario_info in summary.scenario_summaries.items():
                    status_icon = "✅" if scenario_info.get('status') == 'completed' else "❌"
                    status = scenario_info.get('status', 'unknown')
                    duration = safe_format(scenario_info.get('duration_seconds', 0), '.1f')
                    result_files = scenario_info.get('result_files', 0)
                    result_size = safe_format(scenario_info.get('result_size_mb', 0), '.2f')
                    successful_nodes = scenario_info.get('successful_nodes', 0)
                    failed_nodes = scenario_info.get('failed_nodes', 0)
                    error_msg = scenario_info.get('error_message', '')[:50]  # 截断错误信息
                    if len(scenario_info.get('error_message', '')) > 50:
                        error_msg += '...'

                    report_content += f"| {scenario_name} | {status_icon} {status} | {duration} | {result_files} | {result_size} | {successful_nodes} | {failed_nodes} | {error_msg} |\n"

                # 添加健康检查和执行摘要信息
                if summary.health_report:
                    report_content += "\n## 健康检查报告\n\n"
                    for key, value in summary.health_report.items():
                        report_content += f"- **{key}**: {value}\n"

                if summary.execution_summary:
                    report_content += "\n## 执行摘要\n\n"
                    for key, value in summary.execution_summary.items():
                        report_content += f"- **{key}**: {value}\n"

            else:
                # 原有的单场景报告逻辑 (ResultSummary)
                report_content = f"""# 测试结果报告

## 基本信息
- **场景名称**: {summary.scenario_name}
- **测试时间**: {summary.timestamp}
- **总节点数**: {summary.total_nodes}
- **成功节点数**: {summary.successful_nodes}
- **失败节点数**: {summary.failed_nodes}

## 性能指标汇总
- **平均吞吐量**: {safe_format(summary.avg_throughput, '.2f')} tokens/s
- **最大吞吐量**: {safe_format(summary.max_throughput, '.2f')} tokens/s
- **最小吞吐量**: {safe_format(summary.min_throughput, '.2f')} tokens/s
- **平均延迟**: {safe_format(summary.avg_latency, '.3f')} s
- **总请求数**: {summary.total_requests}
- **成功请求数**: {summary.total_successful_requests}
- **总体成功率**: {safe_format(summary.overall_success_rate, '.1f')}%

## 文件统计
- **结果文件数**: {summary.total_result_files}
- **总文件大小**: {safe_format(summary.total_size_mb, '.2f')} MB

## 各节点详细结果

| 节点名称 | 状态 | 吞吐量 | 平均延迟 | 成功率 | 执行时长 |
|---------|------|--------|----------|--------|----------|
"""

                for node_name, result in summary.node_results.items():
                    status_icon = "✅" if result['status'] == 'completed' else "❌"
                    throughput = safe_format(result.get('throughput', 0), '.2f')
                    latency_mean = safe_format(result.get('latency_mean', 0), '.3f')
                    success_rate = safe_format(result.get('success_rate', 0), '.1f')
                    duration = safe_format(result.get('duration', 0), '.1f')
                    report_content += f"| {node_name} | {status_icon} {result['status']} | {throughput} | {latency_mean} | {success_rate}% | {duration}s |\\n"

            markdown_file = result_dir / "test_report.md"
            with open(markdown_file, 'w', encoding='utf-8') as f:
                f.write(report_content)

            self.logger.debug(f"Generated suite report: {markdown_file}")

        except Exception as e:
            self.logger.error(f"Failed to generate suite report: {e}")

    def generate_html_report(self, result_dir: Path, summary: Union[ResultSummary, TestSuiteResultSummary]):
        """生成HTML报告 - 支持单场景和测试套件"""
        try:
            def safe_format(value, format_spec=''):
                if value is None:
                    return 'N/A'
                if format_spec:
                    return f"{value:{format_spec}}"
                return str(value)

            # 检查summary类型并生成相应的HTML报告
            if isinstance(summary, TestSuiteResultSummary):
                # 生成测试套件汇总HTML报告
                html_content = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>测试套件报告 - {summary.suite_name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; line-height: 1.6; }}
        .header {{ background-color: #f4f4f4; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
        .metrics {{ display: flex; justify-content: space-around; margin: 20px 0; flex-wrap: wrap; }}
        .metric {{ text-align: center; padding: 15px; border: 1px solid #ddd; border-radius: 5px; margin: 5px; min-width: 150px; }}
        .metric h3 {{ margin: 0 0 10px 0; color: #333; }}
        .metric p {{ margin: 0; font-size: 1.2em; font-weight: bold; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .success {{ color: green; }}
        .error {{ color: red; }}
        .info-section {{ margin: 20px 0; }}
        .info-section h2 {{ color: #333; border-bottom: 2px solid #ddd; padding-bottom: 5px; }}
        .status-icon {{ font-size: 1.2em; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>测试套件报告 - {summary.suite_name}</h1>
        <p><strong>测试时间:</strong> {summary.timestamp}</p>
        <p><strong>总场景数:</strong> {summary.total_scenarios}</p>
        <p><strong>成功场景数:</strong> {summary.successful_scenarios}</p>
        <p><strong>失败场景数:</strong> {summary.failed_scenarios}</p>
        <p><strong>总体成功率:</strong> {safe_format(summary.overall_success_rate_pct, '.1f')}%</p>
    </div>

    <div class="metrics">
        <div class="metric">
            <h3>总体平均吞吐量</h3>
            <p>{safe_format(summary.overall_avg_throughput, '.2f')} tokens/s</p>
        </div>
        <div class="metric">
            <h3>总体最大吞吐量</h3>
            <p>{safe_format(summary.overall_max_throughput, '.2f')} tokens/s</p>
        </div>
        <div class="metric">
            <h3>总执行时间</h3>
            <p>{safe_format(summary.total_execution_time, '.1f')} 秒</p>
        </div>
        <div class="metric">
            <h3>总结果文件数</h3>
            <p>{summary.total_result_files}</p>
        </div>
        <div class="metric">
            <h3>总文件大小</h3>
            <p>{safe_format(summary.total_size_mb, '.2f')} MB</p>
        </div>
    </div>

    <div class="info-section">
        <h2>各场景详细结果</h2>
        <table>
            <tr>
                <th>场景名称</th>
                <th>状态</th>
                <th>执行时长(秒)</th>
                <th>结果文件数</th>
                <th>文件大小(MB)</th>
                <th>成功节点</th>
                <th>失败节点</th>
                <th>错误信息</th>
            </tr>"""

                for scenario_name, scenario_info in summary.scenario_summaries.items():
                    status_icon = "✅" if scenario_info.get('status') == 'completed' else "❌"
                    status_class = "success" if scenario_info.get('status') == 'completed' else "error"
                    status = scenario_info.get('status', 'unknown')
                    duration = safe_format(scenario_info.get('duration_seconds', 0), '.1f')
                    result_files = scenario_info.get('result_files', 0)
                    result_size = safe_format(scenario_info.get('result_size_mb', 0), '.2f')
                    successful_nodes = scenario_info.get('successful_nodes', 0)
                    failed_nodes = scenario_info.get('failed_nodes', 0)
                    error_msg = scenario_info.get('error_message', '')[:50]  # 截断错误信息
                    if len(scenario_info.get('error_message', '')) > 50:
                        error_msg += '...'

                    html_content += f"""
            <tr>
                <td>{scenario_name}</td>
                <td class="{status_class}"><span class="status-icon">{status_icon}</span> {status}</td>
                <td>{duration}</td>
                <td>{result_files}</td>
                <td>{result_size}</td>
                <td>{successful_nodes}</td>
                <td>{failed_nodes}</td>
                <td>{error_msg}</td>
            </tr>"""

                html_content += """
        </table>
    </div>"""

                # 添加健康检查和执行摘要信息
                if summary.health_report:
                    html_content += """
    <div class="info-section">
        <h2>健康检查报告</h2>
        <table>
            <tr><th>检查项</th><th>状态</th></tr>"""
                    for key, value in summary.health_report.items():
                        html_content += f"<tr><td>{key}</td><td>{value}</td></tr>"
                    html_content += "</table></div>"

                if summary.execution_summary:
                    html_content += """
    <div class="info-section">
        <h2>执行摘要</h2>
        <table>
            <tr><th>项目</th><th>值</th></tr>"""
                    for key, value in summary.execution_summary.items():
                        html_content += f"<tr><td>{key}</td><td>{value}</td></tr>"
                    html_content += "</table></div>"

                html_content += """
</body>
</html>"""

                # 套件报告使用不同的文件名
                html_file = result_dir / "suite_report.html"

            else:
                # 原有的单场景报告逻辑 (ResultSummary)
                html_content = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>测试结果报告 - {summary.scenario_name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; line-height: 1.6; }}
        .header {{ background-color: #f4f4f4; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
        .metrics {{ display: flex; justify-content: space-around; margin: 20px 0; flex-wrap: wrap; }}
        .metric {{ text-align: center; padding: 15px; border: 1px solid #ddd; border-radius: 5px; margin: 5px; min-width: 150px; }}
        .metric h3 {{ margin: 0 0 10px 0; color: #333; }}
        .metric p {{ margin: 0; font-size: 1.2em; font-weight: bold; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .success {{ color: green; }}
        .error {{ color: red; }}
        .status-icon {{ font-size: 1.2em; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>测试结果报告 - {summary.scenario_name}</h1>
        <p><strong>测试时间:</strong> {summary.timestamp}</p>
        <p><strong>总节点数:</strong> {summary.total_nodes}</p>
        <p><strong>成功节点数:</strong> {summary.successful_nodes}</p>
        <p><strong>失败节点数:</strong> {summary.failed_nodes}</p>
    </div>

    <div class="metrics">
        <div class="metric">
            <h3>平均吞吐量</h3>
            <p>{safe_format(summary.avg_throughput, '.2f')} tokens/s</p>
        </div>
        <div class="metric">
            <h3>最大吞吐量</h3>
            <p>{safe_format(summary.max_throughput, '.2f')} tokens/s</p>
        </div>
        <div class="metric">
            <h3>最小吞吐量</h3>
            <p>{safe_format(summary.min_throughput, '.2f')} tokens/s</p>
        </div>
        <div class="metric">
            <h3>平均延迟</h3>
            <p>{safe_format(summary.avg_latency, '.3f')} s</p>
        </div>
        <div class="metric">
            <h3>总请求数</h3>
            <p>{summary.total_requests}</p>
        </div>
        <div class="metric">
            <h3>成功请求数</h3>
            <p>{summary.total_successful_requests}</p>
        </div>
        <div class="metric">
            <h3>总体成功率</h3>
            <p>{safe_format(summary.overall_success_rate, '.1f')}%</p>
        </div>
        <div class="metric">
            <h3>结果文件数</h3>
            <p>{summary.total_result_files}</p>
        </div>
        <div class="metric">
            <h3>总文件大小</h3>
            <p>{safe_format(summary.total_size_mb, '.2f')} MB</p>
        </div>
    </div>

    <h2>节点详细结果</h2>
    <table>
        <tr>
            <th>节点名称</th>
            <th>状态</th>
            <th>吞吐量</th>
            <th>平均延迟</th>
            <th>成功率</th>
            <th>执行时长</th>
        </tr>"""

                for node_name, result in summary.node_results.items():
                    status_class = "success" if result.get('status') == 'completed' else "error"
                    status_icon = "✅" if result.get('status') == 'completed' else "❌"
                    status = result.get('status', 'unknown')
                    throughput = safe_format(result.get('throughput', 0), '.2f')
                    latency_mean = safe_format(result.get('latency_mean', 0), '.3f')
                    success_rate = safe_format(result.get('success_rate', 0), '.1f')
                    duration = safe_format(result.get('duration', 0), '.1f')

                    html_content += f"""
        <tr>
            <td>{node_name}</td>
            <td class="{status_class}"><span class="status-icon">{status_icon}</span> {status}</td>
            <td>{throughput}</td>
            <td>{latency_mean}</td>
            <td>{success_rate}%</td>
            <td>{duration}s</td>
        </tr>"""

                html_content += """
    </table>
</body>
</html>"""

                # 单场景报告使用原有文件名
                html_file = result_dir / "test_report.html"

            # 写入文件
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(html_content)

            self.logger.debug(f"Generated HTML report: {html_file}")

        except Exception as e:
            self.logger.error(f"Failed to generate HTML report: {e}")

    def generate_execution_overview_report(self, result_dir: Path, execution_data: dict):
        """
        生成总体执行概览报告 - Markdown格式

        Args:
            result_dir: 结果目录路径
            execution_data: 执行数据字典，包含execution_summary、health_report、test_suite_summary和validation_results
        """
        try:
            def safe_format(value, format_spec=''):
                if value is None:
                    return 'N/A'
                if format_spec:
                    return f"{value:{format_spec}}"
                return str(value)

            # 从execution_data中提取各个组件
            execution_summary = execution_data.get('execution_summary', {})
            health_report = execution_data.get('health_report', {})
            test_suite_summary = execution_data.get('test_suite_summary')
            validation_results = execution_data.get('validation_results', {})

            # 构建报告内容
            report_content = f"""# 测试套件执行总览报告

生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 🔍 系统健康检查

"""

            # 健康检查信息
            if health_report:
                # 总体状态
                overall_status = health_report.get('overall_status', 'unknown')
                overall_icon = "✅" if overall_status in ['healthy', '正常', 'ok'] else "❌"
                report_content += f"- {overall_icon} **总体状态**: {overall_status}\n"

                # 检查时间
                check_time = health_report.get('check_time', 'N/A')
                if check_time != 'N/A':
                    # 格式化时间显示
                    try:
                        if 'T' in str(check_time):
                            dt = datetime.fromisoformat(str(check_time).replace('Z', '+00:00'))
                            formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            formatted_time = str(check_time)
                    except:
                        formatted_time = str(check_time)
                    report_content += f"- 🕐 **检查时间**: {formatted_time}\n"

                # 组件状态
                components = health_report.get('components', {})
                if components:
                    report_content += f"- 📊 **组件检查** ({len(components)} 个组件):\n"
                    for comp_name, comp_info in components.items():
                        if isinstance(comp_info, dict):
                            comp_status = comp_info.get('status', 'unknown')
                            comp_icon = "✅" if comp_status in ['healthy', '正常', 'ok'] else "❌"
                            report_content += f"  - {comp_icon} **{comp_name}**: {comp_status}\n"
                        else:
                            comp_icon = "✅" if comp_info in ['healthy', '正常', 'ok', True] else "❌"
                            report_content += f"  - {comp_icon} **{comp_name}**: {comp_info}\n"

                # 统计摘要
                summary = health_report.get('summary', {})
                if summary and isinstance(summary, dict):
                    report_content += "- 📈 **状态统计**: "
                    status_parts = []
                    for status, count in summary.items():
                        if count > 0:
                            status_icon = "✅" if status == 'healthy' else ("⚠️" if status == 'degraded' else "❌")
                            status_parts.append(f"{status_icon} {status}: {count}")
                    report_content += ", ".join(status_parts) + "\n"

                # 关键问题
                critical_issues = health_report.get('critical_issues', [])
                if critical_issues:
                    report_content += f"- ⚠️ **关键问题**: {len(critical_issues)} 个问题\n"
                    for issue in critical_issues[:3]:  # 只显示前3个问题
                        report_content += f"  - {issue}\n"
                    if len(critical_issues) > 3:
                        report_content += f"  - ... 还有 {len(critical_issues) - 3} 个问题\n"
                else:
                    report_content += "- ✅ **关键问题**: 无\n"
            else:
                report_content += "- ⚠️ 健康检查信息不可用\n"


            # 验证结果（如果有）
            if validation_results:
                report_content += f"""
## ✅ 配置验证结果

"""
                for node_name, node_data in validation_results.items():
                    # 节点总体状态
                    if isinstance(node_data, dict):
                        overall_status = node_data.get('overall_status', 'unknown')
                        checks = node_data.get('checks', {})

                        # 节点状态图标
                        node_icon = "✅" if overall_status in ['ready', 'passed', 'healthy'] else "❌"
                        report_content += f"- {node_icon} **{node_name}**: {overall_status} ({len(checks)} 项检查)\n"

                        # 详细检查结果
                        for check_name, check_data in checks.items():
                            if isinstance(check_data, dict):
                                check_status = check_data.get('status', 'unknown')
                                check_output = check_data.get('output', '')

                                # 检查项图标
                                check_icon = "✅" if check_status == 'passed' else "❌"

                                # 提取关键信息
                                formatted_info = self._extract_check_info(check_name, check_output)
                                report_content += f"  - {check_icon} **{self._format_check_name(check_name)}**: {formatted_info}\n"
                    else:
                        # 简单值的情况（向后兼容）
                        icon = "✅" if node_data in ['通过', 'passed', True] else "❌"
                        report_content += f"- {icon} **{node_name}**: {node_data}\n"


            # 测试套件结果（如果有）
            if test_suite_summary:
                report_content += f"""
## 📊 测试套件结果汇总

### 基本统计
- **总场景数**: {safe_format(test_suite_summary.total_scenarios)}
- **成功场景数**: {safe_format(test_suite_summary.successful_scenarios)}
- **失败场景数**: {safe_format(test_suite_summary.failed_scenarios)}
- **总体成功率**: {safe_format(test_suite_summary.overall_success_rate, '.1f')}%
- **总执行时间**: {safe_format(test_suite_summary.total_execution_time, '.1f')} 秒

### 性能指标
- **总体平均吞吐量**: {safe_format(test_suite_summary.overall_avg_throughput, '.2f')} tokens/s
- **总体最大吞吐量**: {safe_format(test_suite_summary.overall_max_throughput, '.2f')} tokens/s

### 文件统计
- **总结果文件数**: {safe_format(test_suite_summary.total_result_files)}
- **总文件大小**: {safe_format(test_suite_summary.total_size_mb, '.2f')} MB

### 各场景详细结果

| 场景名称 | 状态 | 执行时长(秒) | 结果文件数 | 文件大小(MB) | 成功节点 | 失败节点 | 错误信息 |
|---------|------|-------------|------------|-------------|----------|----------|----------|
"""

                # 添加各场景的详细信息
                for scenario_name, scenario_info in test_suite_summary.scenario_summaries.items():
                    status_icon = "✅" if scenario_info.get('status') == 'completed' else "❌"
                    status = scenario_info.get('status', 'unknown')
                    duration = safe_format(scenario_info.get('duration_seconds', 0), '.1f')
                    result_files = scenario_info.get('result_files', 0)
                    result_size = safe_format(scenario_info.get('result_size_mb', 0), '.2f')
                    successful_nodes = scenario_info.get('successful_nodes', 0)
                    failed_nodes = scenario_info.get('failed_nodes', 0)
                    error_msg = scenario_info.get('error_message', '')[:50]  # 截断错误信息
                    if len(scenario_info.get('error_message', '')) > 50:
                        error_msg += '...'

                    report_content += f"| {scenario_name} | {status_icon} {status} | {duration} | {result_files} | {result_size} | {successful_nodes} | {failed_nodes} | {error_msg} |\n"


            # 添加页脚信息
            report_content += f"""
---

**报告生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**报告版本**: v2.0
**系统**: Playbook 分布式测试系统
"""

            # 保存报告文件
            report_file = result_dir / "run-all-suite.md"
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report_content)

            self.logger.info(f"Generated execution overview report: {report_file}")

        except Exception as e:
            self.logger.error(f"Failed to generate execution overview report: {e}")
            raise

    def _format_check_name(self, check_name: str) -> str:
        """格式化检查项名称为用户友好的显示"""
        name_mapping = {
            'docker_version': 'Docker版本',
            'gpu_info': 'GPU资源',
            'disk_space': '磁盘空间',
            'memory_info': '内存信息',
            'benchmark_image': '镜像检查',
            'docker_compose_version_node1': 'Docker Compose (node1)',
            'docker_compose_version_node2': 'Docker Compose (node2)',
            'docker_compose_version': 'Docker Compose'
        }
        return name_mapping.get(check_name, check_name.replace('_', ' ').title())

    def _extract_check_info(self, check_name: str, check_output: str) -> str:
        """从检查输出中提取关键信息"""
        if not check_output:
            return "无输出"

        # Docker版本
        if check_name == 'docker_version':
            import re
            match = re.search(r'Docker version (\S+)', check_output)
            return match.group(1) if match else check_output.strip()

        # GPU信息
        elif check_name == 'gpu_info':
            lines = check_output.strip().split('\n')
            gpu_count = len([line for line in lines if 'NVIDIA' in line])
            if gpu_count > 0:
                # 提取GPU型号和内存
                first_gpu = lines[0] if lines else ''
                parts = first_gpu.split(', ')
                if len(parts) >= 2:
                    gpu_model = parts[0].replace('NVIDIA ', '')
                    try:
                        gpu_memory_mb = int(parts[1])
                        gpu_memory = f"{gpu_memory_mb // 1024}GB"
                    except ValueError:
                        gpu_memory = parts[1]
                    return f"{gpu_count}x {gpu_model} ({gpu_memory})"
            return f"{gpu_count} GPU(s)"

        # 磁盘空间
        elif check_name == 'disk_space':
            import re
            # 解析 df 输出
            match = re.search(r'(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\d+)%', check_output)
            if match:
                size = match.group(2)
                used = match.group(3)
                usage = match.group(5)
                return f"{size} 总容量 ({usage}% 使用)"
            return "磁盘信息可用"

        # 内存信息
        elif check_name == 'memory_info':
            import re
            # 解析 free 命令输出
            match = re.search(r'Mem:\s+(\S+)\s+(\S+)\s+(\S+)', check_output)
            if match:
                total = match.group(1)
                used = match.group(2)
                free = match.group(3)
                return f"总计 {total}, 可用 {free}"
            return "内存信息可用"

        # Docker Compose版本
        elif 'docker_compose_version' in check_name:
            import re
            match = re.search(r'version (\S+)', check_output)
            return match.group(1) if match else check_output.strip()

        # 镜像检查
        elif check_name == 'benchmark_image':
            if 'not found' in check_output.lower():
                return "镜像不存在（正常）"
            return check_output.strip()

        # 默认情况
        else:
            # 截断过长的输出
            output = check_output.strip()
            if len(output) > 50:
                return output[:50] + "..."
            return output

    def archive_results(self, result_dir: str, archive_name: Optional[str] = None) -> str:
        """
        归档结果目录

        Args:
            result_dir: 结果目录路径
            archive_name: 归档文件名

        Returns:
            归档文件路径
        """
        result_path = Path(result_dir)
        if not result_path.exists():
            raise ValueError(f"Result directory does not exist: {result_dir}")

        if not archive_name:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"{result_path.name}_{timestamp}.tar.gz"

        archive_path = result_path.parent / archive_name

        self.logger.info(f"Archiving results: {result_dir} -> {archive_path}")

        with tarfile.open(archive_path, 'w:gz') as tar:
            tar.add(result_path, arcname=result_path.name)

        self.logger.info(f"Results archived to: {archive_path}")
        return str(archive_path)