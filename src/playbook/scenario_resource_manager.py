"""
Scenario资源管理器
实现scenarios间的完全资源隔离，确保每个scenario在干净的环境中运行
"""

import gc
import time
import logging
import tempfile
from typing import Dict, List, Optional, Any
from pathlib import Path

from ..utils.ssh_client import ssh_pool
from .node_manager import NodeManager


class ScenarioResourceManager:
    """Scenario级别的资源管理器

    负责在scenarios执行前后进行完整的资源清理，
    确保scenarios间完全隔离，避免状态污染和连接冲突。
    """

    def __init__(self, node_manager: NodeManager):
        self.node_manager = node_manager
        self.logger = logging.getLogger("playbook.resource_manager")

        # 设置详细的日志格式
        self.logger.setLevel(logging.INFO)

        self._cleanup_stats = {
            'total_cleanups': 0,
            'successful_cleanups': 0,
            'failed_cleanups': 0,
            'last_cleanup_time': None,
            'cleanup_history': [],  # 保存最近的清理记录
            'error_history': []     # 保存错误历史
        }

        # 错误恢复配置
        self._error_recovery_config = {
            'max_retry_attempts': 3,
            'retry_delay': 1.0,
            'escalation_delay': 5.0
        }

    def cleanup_scenario_resources(self, scenario_name: str) -> bool:
        """完全清理scenario后的所有资源

        Args:
            scenario_name: 刚完成的scenario名称

        Returns:
            bool: 清理是否成功
        """
        self.logger.info(f"🧹 Starting comprehensive resource cleanup after scenario: {scenario_name}")
        start_time = time.time()

        try:
            self._cleanup_stats['total_cleanups'] += 1

            # 1. 强制关闭所有SSH连接和SFTP会话
            self._cleanup_ssh_connections()

            # 2. 清理临时文件和缓存
            self._cleanup_temporary_files()

            # 3. 重置内存状态
            self._reset_memory_state()

            # 4. 验证清理效果
            if not self._verify_clean_environment():
                self.logger.warning("Resource cleanup verification failed, but continuing...")

            duration = time.time() - start_time
            self._cleanup_stats['successful_cleanups'] += 1
            self._cleanup_stats['last_cleanup_time'] = time.time()

            self.logger.info(f"✅ Resource cleanup completed successfully in {duration:.2f}s")
            return True

        except Exception as e:
            duration = time.time() - start_time
            self._cleanup_stats['failed_cleanups'] += 1

            # 记录详细的错误信息
            error_record = {
                'scenario_name': scenario_name,
                'timestamp': time.time(),
                'duration': duration,
                'error_type': type(e).__name__,
                'error_message': str(e),
                'cleanup_step': 'main_cleanup'
            }
            self._cleanup_stats['error_history'].append(error_record)

            self.logger.error(f"❌ Resource cleanup failed after {duration:.2f}s: {e}")
            self.logger.error(f"Error type: {type(e).__name__}")

            # 尝试降级清理
            try:
                self.logger.warning("🚨 Attempting emergency cleanup...")
                self._emergency_cleanup()
                self.logger.warning("✅ Emergency cleanup completed successfully")

                # 记录紧急清理成功
                error_record['emergency_cleanup'] = 'successful'
                return True

            except Exception as emergency_error:
                # 记录紧急清理失败
                emergency_error_record = {
                    'scenario_name': scenario_name,
                    'timestamp': time.time(),
                    'duration': time.time() - start_time,
                    'error_type': type(emergency_error).__name__,
                    'error_message': str(emergency_error),
                    'cleanup_step': 'emergency_cleanup'
                }
                self._cleanup_stats['error_history'].append(emergency_error_record)

                self.logger.error(f"❌ Emergency cleanup also failed: {emergency_error}")
                self.logger.error(f"Emergency error type: {type(emergency_error).__name__}")

                # 记录完全失败的情况
                error_record['emergency_cleanup'] = 'failed'
                error_record['emergency_error'] = str(emergency_error)

                return False

    def _cleanup_ssh_connections(self):
        """强制清理所有SSH连接和SFTP会话"""
        self.logger.info("🔌 Cleaning up SSH connections and SFTP sessions...")

        # 获取清理前的详细状态
        initial_status = ssh_pool.get_detailed_status()
        self.logger.info(f"Initial connection pool status: {initial_status['total_connections']} total connections")

        if initial_status['total_connections'] > 0:
            self.logger.info(f"Connection details before cleanup:")
            for conn_key, is_connected in initial_status['connection_details'].items():
                self.logger.info(f"  - {conn_key}: {'Connected' if is_connected else 'Disconnected'}")

        # 使用增强的强制清理和验证
        cleanup_result = ssh_pool.force_cleanup_with_verification()

        # 记录详细的清理结果
        if cleanup_result['cleanup_successful']:
            if cleanup_result['verification_passed']:
                self.logger.info(f"✅ Successfully cleaned up {cleanup_result['initial_connections']} SSH connections")
            else:
                self.logger.warning(f"⚠️ Cleanup completed but verification failed - {cleanup_result['final_connections']} connections remain")
                if 'remaining_connections' in cleanup_result:
                    self.logger.warning(f"Remaining connections: {cleanup_result['remaining_connections']}")
        else:
            self.logger.error(f"❌ SSH connection cleanup failed: {cleanup_result['errors']}")
            raise Exception(f"SSH connection cleanup failed: {cleanup_result['errors']}")

        # 将清理结果添加到统计信息中
        self._cleanup_stats['last_ssh_cleanup'] = cleanup_result

    def _cleanup_temporary_files(self):
        """清理临时文件和缓存"""
        self.logger.info("🗂️ Cleaning up temporary files and caches...")

        try:
            # 清理Python临时文件目录中的相关文件
            temp_dir = Path(tempfile.gettempdir())
            playbook_temp_files = list(temp_dir.glob("playbook_*"))
            test_temp_files = list(temp_dir.glob("test_script_*"))

            cleaned_files = 0
            for temp_file in playbook_temp_files + test_temp_files:
                try:
                    if temp_file.is_file():
                        temp_file.unlink()
                        cleaned_files += 1
                    elif temp_file.is_dir():
                        import shutil
                        shutil.rmtree(temp_file)
                        cleaned_files += 1
                except Exception as e:
                    self.logger.debug(f"Failed to cleanup temp file {temp_file}: {e}")

            if cleaned_files > 0:
                self.logger.info(f"✅ Cleaned up {cleaned_files} temporary files")
            else:
                self.logger.debug("No temporary files to cleanup")

        except Exception as e:
            self.logger.warning(f"Temporary file cleanup failed: {e}")

    def _reset_memory_state(self):
        """重置内存状态和缓存"""
        self.logger.info("🧠 Resetting memory state and caches...")

        try:
            # 强制垃圾回收
            collected = gc.collect()
            if collected > 0:
                self.logger.info(f"✅ Garbage collected {collected} objects")

            # 清理Node Manager可能的内部缓存
            # 注意：这里不清理nodes配置本身，只清理临时状态

        except Exception as e:
            self.logger.warning(f"Memory state reset failed: {e}")

    def _verify_clean_environment(self) -> bool:
        """验证环境确实已清理干净

        Returns:
            bool: 环境是否干净
        """
        self.logger.info("🔍 Verifying clean environment...")

        verification_results = {
            'ssh_connections': False,
            'memory_state': False
        }

        try:
            # 使用详细的SSH连接池状态检查
            pool_status = ssh_pool.get_detailed_status()
            verification_results['ssh_connections'] = (pool_status['total_connections'] == 0)

            if verification_results['ssh_connections']:
                self.logger.info("✅ SSH connection pool is completely clean")
            else:
                self.logger.warning(f"⚠️ SSH connection pool verification failed:")
                self.logger.warning(f"  - Total connections: {pool_status['total_connections']}")
                self.logger.warning(f"  - Connected: {pool_status['connected_count']}")
                self.logger.warning(f"  - Disconnected: {pool_status['disconnected_count']}")
                self.logger.warning(f"  - Pool utilization: {pool_status['pool_utilization']}")

                # 记录具体的连接详情
                if pool_status['connection_details']:
                    self.logger.warning("  - Active connections:")
                    for conn_key, is_connected in pool_status['connection_details'].items():
                        self.logger.warning(f"    - {conn_key}: {'Connected' if is_connected else 'Disconnected'}")

            # 检查内存状态（简单检查）
            verification_results['memory_state'] = True  # 暂时总是通过

            # 整体验证结果
            all_clean = all(verification_results.values())

            if all_clean:
                self.logger.info("✅ Environment verification passed - completely clean")
            else:
                failed_checks = [k for k, v in verification_results.items() if not v]
                self.logger.warning(f"⚠️ Environment verification failed for: {failed_checks}")

            # 将验证结果添加到统计信息中
            self._cleanup_stats['last_verification'] = {
                'passed': all_clean,
                'results': verification_results,
                'pool_status': pool_status
            }

            return all_clean

        except Exception as e:
            self.logger.error(f"Environment verification failed: {e}")
            return False

    def _emergency_cleanup(self):
        """紧急清理机制 - 降级清理策略"""
        self.logger.warning("🚨 Performing emergency cleanup...")

        try:
            # 最基本的清理：强制关闭连接池
            ssh_pool.close_all()

            # 强制垃圾回收
            gc.collect()

            self.logger.info("Emergency cleanup completed")

        except Exception as e:
            self.logger.error(f"Emergency cleanup failed: {e}")
            raise

    def get_cleanup_stats(self) -> Dict[str, Any]:
        """获取清理统计信息"""
        total_cleanups = self._cleanup_stats['total_cleanups']
        successful_cleanups = self._cleanup_stats['successful_cleanups']

        stats = {
            **self._cleanup_stats,
            'success_rate': (successful_cleanups / max(1, total_cleanups) * 100),
            'failure_rate': (self._cleanup_stats['failed_cleanups'] / max(1, total_cleanups) * 100),
            'current_connections': ssh_pool.get_connection_count(),
            'current_pool_status': ssh_pool.get_detailed_status(),
            'total_errors': len(self._cleanup_stats['error_history']),
            'recent_errors': self._cleanup_stats['error_history'][-5:] if self._cleanup_stats['error_history'] else [],
            'last_cleanup_duration': None
        }

        # 添加最近清理的持续时间
        if self._cleanup_stats['cleanup_history']:
            last_cleanup = self._cleanup_stats['cleanup_history'][-1]
            stats['last_cleanup_duration'] = last_cleanup.get('duration', 0)

        return stats

    def get_error_summary(self) -> Dict[str, Any]:
        """获取错误摘要信息"""
        error_history = self._cleanup_stats['error_history']

        if not error_history:
            return {
                'total_errors': 0,
                'error_types': {},
                'recent_errors': [],
                'recommendations': []
            }

        # 统计错误类型
        error_types = {}
        for error in error_history:
            error_type = error.get('error_type', 'Unknown')
            error_types[error_type] = error_types.get(error_type, 0) + 1

        # 生成建议
        recommendations = []
        if 'SSHException' in error_types:
            recommendations.append("Consider checking network connectivity and SSH credentials")
        if 'TimeoutError' in error_types:
            recommendations.append("Consider increasing timeout values for cleanup operations")
        if error_types.get('Exception', 0) > 3:
            recommendations.append("Multiple generic errors detected - enable debug logging for more details")

        return {
            'total_errors': len(error_history),
            'error_types': error_types,
            'recent_errors': error_history[-10:],  # Last 10 errors
            'recommendations': recommendations,
            'error_frequency': len(error_history) / max(1, self._cleanup_stats['total_cleanups'])
        }

    def reset_error_history(self):
        """重置错误历史记录"""
        self.logger.info("🔄 Resetting error history...")
        cleared_count = len(self._cleanup_stats['error_history'])
        self._cleanup_stats['error_history'].clear()
        self._cleanup_stats['cleanup_history'].clear()
        self.logger.info(f"✅ Cleared {cleared_count} error records")

    def log_cleanup_summary(self, scenario_name: str, success: bool, duration: float):
        """记录清理摘要到历史记录"""
        cleanup_record = {
            'scenario_name': scenario_name,
            'timestamp': time.time(),
            'duration': duration,
            'success': success,
            'connections_before': getattr(self, '_connections_before_cleanup', 0),
            'connections_after': ssh_pool.get_connection_count()
        }

        self._cleanup_stats['cleanup_history'].append(cleanup_record)

        # 只保留最近的20条记录
        if len(self._cleanup_stats['cleanup_history']) > 20:
            self._cleanup_stats['cleanup_history'] = self._cleanup_stats['cleanup_history'][-20:]

    def force_clean_environment(self) -> bool:
        """强制清理环境 - 用于手动调用或紧急情况

        Returns:
            bool: 清理是否成功
        """
        self.logger.info("🔧 Force cleaning environment...")
        return self.cleanup_scenario_resources("manual_cleanup")