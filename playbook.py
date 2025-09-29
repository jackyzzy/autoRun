#!/usr/bin/env python3
"""
Playbook主程序
分布式推理引擎自动化测试系统的命令行入口
"""

import sys
import os
import signal
from pathlib import Path

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import click
import yaml
import json
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich import print as rprint

from src.playbook.core import PlaybookCore, ExecutionResult


# 全局变量
console = Console()
playbook_core = None


def signal_handler(sig, frame):
    """信号处理器，优雅退出"""
    console.print("\n[yellow]Received interrupt signal, cleaning up...[/yellow]")
    if playbook_core:
        playbook_core.cleanup()
    sys.exit(0)


# 注册信号处理器
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


@click.group()
@click.option('--config-dir', '-c', default='config', help='Configuration directory')
@click.option('--scenarios-dir', '-s', default='config/scenarios', help='Scenarios directory')
@click.option('--results-dir', '-r', default='results', help='Results directory')
@click.option('--log-level', '-l', default='INFO', 
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
              help='Log level')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
@click.pass_context
def cli(ctx, config_dir, scenarios_dir, results_dir, log_level, verbose):
    """
    分布式推理引擎自动化测试Playbook
    
    这个工具可以自动化地在多个节点上执行推理引擎测试，
    收集性能数据，并生成详细的测试报告。
    """
    ctx.ensure_object(dict)
    ctx.obj['config_dir'] = config_dir
    ctx.obj['scenarios_dir'] = scenarios_dir
    ctx.obj['results_dir'] = results_dir
    ctx.obj['log_level'] = log_level
    ctx.obj['verbose'] = verbose
    
    # 显示欢迎信息
    if verbose:
        console.print(Panel.fit(
            "🚀 [bold blue]分布式推理引擎自动化测试Playbook[/bold blue] 🚀\n\n"
            f"配置目录: {config_dir}\n"
            f"场景目录: {scenarios_dir}\n"
            f"结果目录: {results_dir}\n"
            f"日志级别: {log_level}",
            title="Playbook v1.0"
        ))


def get_playbook_core(ctx):
    """获取Playbook核心实例"""
    global playbook_core
    if playbook_core is None:
        playbook_core = PlaybookCore(
            config_dir=ctx.obj['config_dir'],
            scenarios_dir=ctx.obj['scenarios_dir'],
            results_dir=ctx.obj['results_dir'],
            log_level=ctx.obj['log_level']
        )
    return playbook_core


@cli.command()
@click.pass_context
def status(ctx):
    """显示系统状态"""
    with console.status("[bold green]Checking system status...") as status:
        try:
            core = get_playbook_core(ctx)
            system_status = core.get_system_status()
            
            # 显示整体状态
            overall_status = system_status['overall_status']
            status_color = {
                'healthy': 'green',
                'degraded': 'yellow', 
                'unhealthy': 'red',
                'unknown': 'gray'
            }.get(overall_status, 'gray')
            
            console.print(f"\n[bold]Overall Status:[/bold] [{status_color}]{overall_status.upper()}[/{status_color}]")
            
            # 显示组件状态
            components = system_status.get('components', {})
            
            table = Table(title="Component Status")
            table.add_column("Component", style="cyan")
            table.add_column("Status", justify="center")
            table.add_column("Details", style="dim")
            
            if 'nodes' in components:
                node_info = components['nodes']
                table.add_row(
                    "Nodes",
                    f"[green]{node_info['enabled_nodes']}/{node_info['total_nodes']} enabled[/green]",
                    f"Pool size: {node_info.get('connection_pool_size', 0)}"
                )
            
            if 'scenarios' in components:
                scenario_info = components['scenarios']
                table.add_row(
                    "Scenarios", 
                    f"[blue]{scenario_info['enabled_scenarios']}/{scenario_info['total_scenarios']} enabled[/blue]",
                    f"Mode: {scenario_info.get('execution_mode', 'unknown')}"
                )
            
            if 'docker' in components:
                docker_info = components['docker']
                table.add_row(
                    "Docker Services",
                    f"[yellow]{docker_info.get('running_services', 0)} running[/yellow]",
                    f"Total: {docker_info.get('total_services', 0)}"
                )
            
            # 显示运行状态
            if system_status.get('is_running', False):
                current = system_status.get('current_scenario', 'unknown')
                table.add_row(
                    "Execution",
                    "[green]RUNNING[/green]",
                    f"Current: {current}"
                )
            else:
                table.add_row(
                    "Execution",
                    "[gray]IDLE[/gray]",
                    "Ready to run"
                )
            
            console.print(table)
            
            # 显示健康问题
            health = system_status.get('health', {})
            critical_issues = health.get('critical_issues', [])
            if critical_issues:
                console.print("\n[bold red]Critical Issues:[/bold red]")
                for issue in critical_issues[:5]:  # 只显示前5个
                    console.print(f"  • [red]{issue}[/red]")
                    
        except Exception as e:
            console.print(f"[red]Error getting system status: {e}[/red]")


@cli.command()
@click.pass_context
def scenarios(ctx):
    """列出所有测试场景"""
    try:
        core = get_playbook_core(ctx)
        scenario_info = core.list_scenarios()
        
        table = Table(title=f"Test Scenarios ({scenario_info['enabled_scenarios']}/{scenario_info['total_scenarios']} enabled)")
        table.add_column("Name", style="cyan")
        table.add_column("Status", justify="center")
        table.add_column("Description", style="dim")
        table.add_column("Duration", justify="right")
        table.add_column("Valid", justify="center")
        
        for name, scenario in scenario_info['scenarios'].items():
            status_text = "[green]✓ Enabled[/green]" if scenario['enabled'] else "[red]✗ Disabled[/red]"
            valid_text = "[green]✓[/green]" if scenario['is_valid'] else "[red]✗[/red]"
            
            duration = ""
            if scenario['metadata'] and scenario['metadata'].get('estimated_duration'):
                duration = f"{scenario['metadata']['estimated_duration'] // 60}m"
            
            table.add_row(
                name,
                status_text,
                scenario['description'][:50] + "..." if len(scenario['description']) > 50 else scenario['description'],
                duration,
                valid_text
            )
        
        console.print(table)
        
        # 显示执行顺序
        if scenario_info['execution_order']:
            console.print(f"\n[bold]Execution Order:[/bold] {' → '.join(scenario_info['execution_order'])}")
        
    except Exception as e:
        console.print(f"[red]Error listing scenarios: {e}[/red]")


@cli.command()
@click.pass_context
def nodes(ctx):
    """列出所有节点"""
    try:
        core = get_playbook_core(ctx)
        
        with console.status("[bold green]Checking node connectivity..."):
            node_info = core.list_nodes()
        
        table = Table(title=f"Nodes ({node_info['enabled_nodes']}/{node_info['total_nodes']} enabled)")
        table.add_column("Name", style="cyan")
        table.add_column("Host", style="blue")
        table.add_column("Status", justify="center")
        table.add_column("Role", style="green")
        table.add_column("Tags", style="dim")
        
        connectivity = node_info.get('connectivity', {})
        
        for name, node in node_info['nodes'].items():
            status_text = "[green]✓ Enabled[/green]" if node['enabled'] else "[red]✗ Disabled[/red]"
            
            # 添加连接状态
            if name in connectivity:
                if connectivity[name]:
                    status_text += " [green]●[/green]"
                else:
                    status_text += " [red]●[/red]"
            
            tags_text = ", ".join(node.get('tags', []))
            
            table.add_row(
                name,
                node['host'],
                status_text,
                node.get('role', 'unknown'),
                tags_text
            )
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]Error listing nodes: {e}[/red]")


@cli.command()
@click.argument('scenario_name', required=False)
@click.option('--all', '-a', is_flag=True, help='Run all enabled scenarios')
@click.option('--dry-run', is_flag=True, help='Validate configuration without running')
@click.pass_context
def run(ctx, scenario_name, all, dry_run):
    """运行测试场景"""
    try:
        core = get_playbook_core(ctx)
        
        if dry_run:
            console.print("[yellow]Performing dry run - validating configuration...[/yellow]")
            validation = core.validate_configuration()
            
            if validation['overall_valid']:
                console.print("[green]✓ Configuration is valid[/green]")
            else:
                console.print("[red]✗ Configuration has issues:[/red]")
                for issue in validation['issues']:
                    console.print(f"  • [red]{issue}[/red]")
                    
            if validation['warnings']:
                console.print("[yellow]Warnings:[/yellow]")
                for warning in validation['warnings']:
                    console.print(f"  • [yellow]{warning}[/yellow]")
            
            return
        
        if all:
            console.print("[bold green]Running all enabled scenarios...[/bold green]")
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                console=console
            ) as progress:
                task = progress.add_task("Executing test suite...", total=100)
                
                results = core.run_full_test_suite()
                progress.update(task, completed=100)
            
            # 显示结果
            if results.status == 'completed':
                summary = results.data['execution_summary']
                console.print(f"\n[green]✓ Test suite completed![/green]")
                console.print(f"Success rate: {summary['success_rate']:.1f}%")
                console.print(f"Total scenarios: {summary['total']}")
                console.print(f"Completed: {summary['completed']}")
                console.print(f"Failed: {summary['failed']}")
                if summary.get('skipped', 0) > 0:
                    console.print(f"[yellow]Skipped: {summary['skipped']}[/yellow]")
                if summary.get('cancelled', 0) > 0:
                    console.print(f"[dim]Cancelled: {summary['cancelled']}[/dim]")
            else:
                console.print(f"[red]✗ Test suite failed: {results.error or 'Unknown error'}[/red]")
                
        elif scenario_name:
            console.print(f"[bold green]Running scenario: {scenario_name}[/bold green]")
            
            with console.status(f"[bold green]Executing {scenario_name}..."):
                results = core.run_single_scenario(scenario_name)
            
            if results.status == 'completed':
                console.print(f"[green]✓ Scenario {scenario_name} completed successfully![/green]")
            elif results.status == 'skipped':
                console.print(f"[yellow]⊘ Scenario {scenario_name} was skipped: {results.message or 'Unknown'}[/yellow]")
            else:
                console.print(f"[red]✗ Scenario {scenario_name} failed: {results.error or 'Unknown error'}[/red]")
        else:
            console.print("[red]Please specify a scenario name or use --all flag[/red]")
            
    except Exception as e:
        console.print(f"[red]Error running scenarios: {e}[/red]")


@cli.command()
@click.argument('scenario_name')
@click.option('--enable', is_flag=True, help='Enable the scenario')
@click.option('--disable', is_flag=True, help='Disable the scenario')
@click.pass_context
def scenario(ctx, scenario_name, enable, disable):
    """管理单个场景"""
    try:
        core = get_playbook_core(ctx)
        
        if enable and disable:
            console.print("[red]Cannot both enable and disable a scenario[/red]")
            return
        
        if enable:
            success = core.enable_scenario(scenario_name)
            if success:
                console.print(f"[green]✓ Enabled scenario: {scenario_name}[/green]")
            else:
                console.print(f"[red]✗ Failed to enable scenario: {scenario_name}[/red]")
                
        elif disable:
            success = core.disable_scenario(scenario_name)
            if success:
                console.print(f"[yellow]⊘ Disabled scenario: {scenario_name}[/yellow]")
            else:
                console.print(f"[red]✗ Failed to disable scenario: {scenario_name}[/red]")
        else:
            # 显示场景详细信息
            scenario_info = core.list_scenarios()
            scenario = scenario_info['scenarios'].get(scenario_name)
            
            if not scenario:
                console.print(f"[red]Scenario not found: {scenario_name}[/red]")
                return
            
            console.print(Panel.fit(
                f"[bold]{scenario['name']}[/bold]\n\n"
                f"描述: {scenario['description']}\n"
                f"目录: {scenario['directory']}\n"
                f"状态: {'✓ 启用' if scenario['enabled'] else '✗ 禁用'}\n"
                f"有效: {'✓' if scenario['is_valid'] else '✗'}\n" +
                (f"预估时长: {scenario['metadata']['estimated_duration'] // 60}分钟\n" 
                 if scenario['metadata'] and scenario['metadata'].get('estimated_duration') else "") +
                (f"标签: {', '.join(scenario['metadata']['tags'])}\n"
                 if scenario['metadata'] and scenario['metadata'].get('tags') else ""),
                title=f"场景详情: {scenario_name}"
            ))
            
    except Exception as e:
        console.print(f"[red]Error managing scenario: {e}[/red]")


@cli.command()
@click.pass_context
def health(ctx):
    """运行系统健康检查"""
    try:
        core = get_playbook_core(ctx)
        
        with console.status("[bold green]Running health checks..."):
            health_results = core.health_checker.run_all_checks()
            health_report = core.health_checker.get_health_report()
        
        overall_status = health_report['overall_status']
        status_color = {
            'healthy': 'green',
            'degraded': 'yellow',
            'unhealthy': 'red',
            'unknown': 'gray'
        }.get(overall_status, 'gray')
        
        console.print(f"\n[bold]Overall Health:[/bold] [{status_color}]{overall_status.upper()}[/{status_color}]")
        
        # 显示各项检查结果
        table = Table(title="Health Check Results")
        table.add_column("Check", style="cyan")
        table.add_column("Status", justify="center")
        table.add_column("Details", style="dim")
        
        for component_name, component_health in health_results.items():
            status_icon = {
                'healthy': '[green]✓[/green]',
                'degraded': '[yellow]⚠[/yellow]',
                'unhealthy': '[red]✗[/red]',
                'unknown': '[gray]?[/gray]'
            }.get(component_health.status.value, '[gray]?[/gray]')
            
            details = ""
            if component_health.error_messages:
                details = component_health.error_messages[0][:50]
                if len(component_health.error_messages[0]) > 50:
                    details += "..."
            
            table.add_row(component_name.replace('_', ' ').title(), status_icon, details)
        
        console.print(table)
        
        # 显示关键问题
        if health_report.get('critical_issues'):
            console.print("\n[bold red]Critical Issues:[/bold red]")
            for issue in health_report['critical_issues'][:5]:
                console.print(f"  • [red]{issue}[/red]")
        
    except Exception as e:
        console.print(f"[red]Error running health checks: {e}[/red]")


@cli.command()
@click.pass_context 
def validate(ctx):
    """验证配置文件"""
    try:
        core = get_playbook_core(ctx)
        
        console.print("[bold blue]Validating configuration...[/bold blue]")
        validation = core.validate_configuration()
        
        if validation['overall_valid']:
            console.print("[green]✓ Configuration is valid[/green]")
        else:
            console.print("[red]✗ Configuration has issues:[/red]")
            for issue in validation['issues']:
                console.print(f"  • [red]{issue}[/red]")
        
        if validation['warnings']:
            console.print("[yellow]Warnings:[/yellow]")
            for warning in validation['warnings']:
                console.print(f"  • [yellow]{warning}[/yellow]")
        
        console.print(f"\nTotal issues: {len(validation['issues'])}")
        console.print(f"Total warnings: {len(validation['warnings'])}")
        
    except Exception as e:
        console.print(f"[red]Error validating configuration: {e}[/red]")


@cli.command(name='validate-env')
@click.option('--scenario', '-s', help='Validate environment variables for specific scenario')
@click.option('--all', '-a', is_flag=True, help='Validate environment variables for all scenarios')
@click.option('--format', '-f', type=click.Choice(['table', 'json', 'yaml']), 
              default='table', help='Output format')
@click.pass_context
def validate_env(ctx, scenario, all, format):
    """验证scenario环境变量配置"""
    try:
        core = get_playbook_core(ctx)
        scenario_manager = core.scenario_manager
        
        if all:
            console.print("[bold blue]Validating environment variables for all scenarios...[/bold blue]")
            scenarios_with_env = scenario_manager.get_scenarios_with_env_files()
            
            if not scenarios_with_env:
                console.print("[yellow]No scenarios with environment files found[/yellow]")
                return
            
            validation_results = {}
            for scenario_name in scenarios_with_env.keys():
                validation_results[scenario_name] = scenario_manager.validate_env_file(scenario_name)
            
            if format == 'json':
                console.print(json.dumps(validation_results, indent=2, ensure_ascii=False))
            elif format == 'yaml':
                console.print(yaml.dump(validation_results, default_flow_style=False, allow_unicode=True))
            else:
                # 表格格式
                table = Table(title="Environment Variables Validation Results")
                table.add_column("Scenario", style="cyan")
                table.add_column("Status", justify="center")
                table.add_column("Env File", justify="center")
                table.add_column("Errors", justify="center", style="red")
                table.add_column("Warnings", justify="center", style="yellow")
                table.add_column("Missing Vars", justify="center", style="magenta")
                
                for scenario_name, result in validation_results.items():
                    status_icon = "[green]✓[/green]" if result['is_valid'] else "[red]✗[/red]"
                    env_file_icon = "[green]✓[/green]" if result['has_env_file'] else "[gray]✗[/gray]"
                    
                    table.add_row(
                        scenario_name,
                        status_icon,
                        env_file_icon,
                        str(len(result['errors'])),
                        str(len(result['warnings'])),
                        str(len(result['missing_vars']))
                    )
                
                console.print(table)
                
                # 显示详细错误信息
                for scenario_name, result in validation_results.items():
                    if result['errors'] or result['warnings']:
                        console.print(f"\n[bold]{scenario_name}[/bold]:")
                        if result['errors']:
                            console.print("  [red]Errors:[/red]")
                            for error in result['errors']:
                                console.print(f"    • [red]{error}[/red]")
                        if result['warnings']:
                            console.print("  [yellow]Warnings:[/yellow]")
                            for warning in result['warnings']:
                                console.print(f"    • [yellow]{warning}[/yellow]")
                        if result['missing_vars']:
                            console.print("  [magenta]Missing Variables:[/magenta]")
                            for var in result['missing_vars']:
                                console.print(f"    • [magenta]{var}[/magenta]")
        
        elif scenario:
            console.print(f"[bold blue]Validating environment variables for scenario: {scenario}[/bold blue]")
            result = scenario_manager.validate_env_file(scenario)
            
            if format == 'json':
                console.print(json.dumps(result, indent=2, ensure_ascii=False))
            elif format == 'yaml':
                console.print(yaml.dump(result, default_flow_style=False, allow_unicode=True))
            else:
                # 详细显示单个scenario的验证结果
                status_color = "green" if result['is_valid'] else "red"
                status_text = "VALID" if result['is_valid'] else "INVALID"
                console.print(f"\n[bold]Status:[/bold] [{status_color}]{status_text}[/{status_color}]")
                
                if not result['has_env_file']:
                    console.print("[yellow]⚠ No environment file (.env) found[/yellow]")
                    console.print("  Consider creating an .env file for this scenario")
                    return
                
                console.print(f"[bold]Environment File:[/bold] {result['env_file_path']}")
                console.print(f"[bold]Variables Found:[/bold] {len(result['env_vars'])}")
                
                if result['env_vars']:
                    console.print("\n[bold]Environment Variables:[/bold]")
                    for key, value in result['env_vars'].items():
                        # 隐藏敏感信息
                        if any(sensitive in key.lower() for sensitive in ['password', 'secret', 'token', 'key']):
                            display_value = "***"
                        else:
                            display_value = value[:50] + "..." if len(value) > 50 else value
                        console.print(f"  {key}={display_value}")
                
                if result['errors']:
                    console.print("\n[bold red]Errors:[/bold red]")
                    for error in result['errors']:
                        console.print(f"  • [red]{error}[/red]")
                
                if result['warnings']:
                    console.print("\n[bold yellow]Warnings:[/bold yellow]")
                    for warning in result['warnings']:
                        console.print(f"  • [yellow]{warning}[/yellow]")
                
                if result['missing_vars']:
                    console.print("\n[bold magenta]Missing Variables (referenced in docker-compose.yml):[/bold magenta]")
                    for var in result['missing_vars']:
                        console.print(f"  • [magenta]{var}[/magenta]")
                    console.print("\n[dim]These variables are used in docker-compose.yml but not defined in .env file")
                    console.print("Consider adding them to the .env file or defining them as system environment variables[/dim]")
        
        else:
            console.print("[red]Please specify --scenario SCENARIO_NAME or use --all flag[/red]")
            console.print("\nExamples:")
            console.print("  playbook validate-env --scenario 001_baseline")
            console.print("  playbook validate-env --all")
            console.print("  playbook validate-env --scenario 001_baseline --format json")
        
    except Exception as e:
        console.print(f"[red]Error validating environment variables: {e}[/red]")


@cli.command()
@click.option('--format', '-f', type=click.Choice(['json', 'yaml', 'table']), 
              default='table', help='Output format')
@click.pass_context
def results(ctx, format):
    """查看测试结果"""
    try:
        core = get_playbook_core(ctx)
        result_dirs = core.result_collector.list_result_directories()
        
        if format == 'json':
            console.print(json.dumps(result_dirs, indent=2, ensure_ascii=False))
        elif format == 'yaml':
            console.print(yaml.dump(result_dirs, default_flow_style=False, allow_unicode=True))
        else:
            if not result_dirs:
                console.print("[yellow]No test results found[/yellow]")
                return
            
            table = Table(title="Test Results")
            table.add_column("Name", style="cyan")
            table.add_column("Modified", style="blue")
            table.add_column("Files", justify="right")
            table.add_column("Size", justify="right", style="green")
            table.add_column("Summary", justify="center")
            
            for result_dir in result_dirs[:10]:  # 只显示最新10个
                size_mb = result_dir.get('total_size_mb', 0)
                size_text = f"{size_mb:.1f} MB" if size_mb > 0 else "-"
                
                summary_icon = "[green]✓[/green]" if result_dir.get('has_summary') else "[gray]?[/gray]"
                
                modified_time = result_dir.get('modified_time', '')[:16].replace('T', ' ')
                
                table.add_row(
                    result_dir['name'],
                    modified_time,
                    str(result_dir.get('file_count', 0)),
                    size_text,
                    summary_icon
                )
            
            console.print(table)
        
    except Exception as e:
        console.print(f"[red]Error listing results: {e}[/red]")


@cli.command()
@click.option('--port', '-p', default=8888, help='Web server port')
@click.option('--host', '-h', default='0.0.0.0', help='Web server host')
@click.pass_context
def webui(ctx, port, host):
    """启动Web UI界面（未实现）"""
    console.print(f"[yellow]Web UI feature is not implemented yet[/yellow]")
    console.print(f"Planned to start web interface at http://{host}:{port}")


if __name__ == '__main__':
    try:
        cli()
    except KeyboardInterrupt:
        console.print("\n[yellow]Operation cancelled by user[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error: {e}[/red]")
        sys.exit(1)
    finally:
        if playbook_core:
            playbook_core.cleanup()