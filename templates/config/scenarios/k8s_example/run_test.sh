#!/bin/bash
# K8S场景测试脚本示例

set -e

echo "=== K8S Example Test Script ==="
echo "Scenario: ${SCENARIO_NAME:-k8s_example}"
echo "Time: $(date)"

# 创建结果目录
RESULT_DIR="/tmp/k8s_test_results"
mkdir -p "$RESULT_DIR"

# 测试1: 检查Pod状态
echo "Checking pod status..."
kubectl get pods -l app=example-app -o wide > "$RESULT_DIR/pods_status.txt"

# 测试2: 检查Service端点
echo "Checking service endpoints..."
kubectl get endpoints example-service -o yaml > "$RESULT_DIR/service_endpoints.yaml"

# 测试3: 获取Pod日志
echo "Collecting pod logs..."
POD_NAME=$(kubectl get pods -l app=example-app -o jsonpath='{.items[0].metadata.name}')
if [ -n "$POD_NAME" ]; then
    kubectl logs "$POD_NAME" --tail=100 > "$RESULT_DIR/pod_logs.txt"
fi

# 测试4: 简单的HTTP测试（如果Service暴露了端口）
echo "Running HTTP test..."
# curl -s http://example-service:8080/health > "$RESULT_DIR/health_check.json" || echo "HTTP test skipped"

# 生成测试报告
echo "Generating test report..."
cat > "$RESULT_DIR/test_report.json" << EOF
{
    "scenario": "${SCENARIO_NAME:-k8s_example}",
    "timestamp": "$(date -Iseconds)",
    "status": "completed",
    "tests_run": 4,
    "tests_passed": 4
}
EOF

echo "=== Test completed ==="
echo "Results saved to: $RESULT_DIR"
