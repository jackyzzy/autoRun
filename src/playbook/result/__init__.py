"""
结果收集模块

提供分布式测试结果的收集、传输、分析和报告功能
"""

from .result_collector import ResultCollector
from .result_transporter import ResultTransporter  
from .result_analyzer import ResultAnalyzer
from .result_reporter import ResultReporter
from .result_models import (
    CollectionMode, CollectionTask, CollectionSummary, ResultSummary
)

__all__ = [
    'ResultCollector',
    'ResultTransporter', 
    'ResultAnalyzer',
    'ResultReporter',
    'CollectionMode',
    'CollectionTask', 
    'CollectionSummary',
    'ResultSummary'
]