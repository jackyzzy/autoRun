"""
结果报告生成器
负责生成各种格式的测试结果报告
"""

import json
import yaml
import tarfile
import logging
from typing import Dict, List, Optional, Any
from pathlib import Path
from datetime import datetime

from .result_models import CollectionSummary, ResultSummary


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

            markdown_file = result_dir / "test_report.md"
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

    def generate_markdown_report_legacy(self, result_dir: Path, summary: ResultSummary):
        """生成Markdown报告 - 兼容旧版本"""
        try:
            def safe_format(value, format_spec=''):
                if value is None:
                    return 'N/A'
                if format_spec:
                    return f"{value:{format_spec}}"
                return str(value)

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
            
            self.logger.debug(f"Generated Markdown report: {markdown_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to generate Markdown report: {e}")

    def generate_html_report(self, result_dir: Path, summary: ResultSummary):
        """生成HTML报告"""
        try:
            html_content = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>测试结果报告 - {summary.scenario_name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f4f4f4; padding: 20px; border-radius: 5px; }}
        .metrics {{ display: flex; justify-content: space-around; margin: 20px 0; }}
        .metric {{ text-align: center; padding: 10px; border: 1px solid #ddd; border-radius: 5px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .success {{ color: green; }}
        .error {{ color: red; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>测试结果报告 - {summary.scenario_name}</h1>
        <p><strong>测试时间:</strong> {summary.timestamp}</p>
        <p><strong>总节点数:</strong> {summary.total_nodes}</p>
    </div>
    
    <div class="metrics">
        <div class="metric">
            <h3>平均吞吐量</h3>
            <p>{summary.avg_throughput:.2f} tokens/s</p>
        </div>
        <div class="metric">
            <h3>平均延迟</h3>
            <p>{summary.avg_latency:.3f} s</p>
        </div>
        <div class="metric">
            <h3>成功率</h3>
            <p>{summary.overall_success_rate:.1f}%</p>
        </div>
    </div>
    
    <h2>节点详细结果</h2>
    <table>
        <tr>
            <th>节点名称</th>
            <th>状态</th>
            <th>吞吐量</th>
            <th>延迟</th>
            <th>成功率</th>
        </tr>
"""
            
            for node_name, result in summary.node_results.items():
                status_class = "success" if result.get('status') == 'completed' else "error"
                html_content += f"""
        <tr>
            <td>{node_name}</td>
            <td class="{status_class}">{result.get('status', 'unknown')}</td>
            <td>{result.get('throughput', 0):.2f}</td>
            <td>{result.get('latency_mean', 0):.3f}</td>
            <td>{result.get('success_rate', 0):.1f}%</td>
        </tr>"""
            
            html_content += """
    </table>
</body>
</html>"""
            
            html_file = result_dir / "test_report.html"
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            self.logger.debug(f"Generated HTML report: {html_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to generate HTML report: {e}")

    def archive_results(self, result_dir: str, archive_name: str = None) -> str:
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