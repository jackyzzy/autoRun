DIST_DIR = dist
BUILD_DIR = build
TEMP_DIR = temp

# 清理生成的文件
clean:
	rm -rf venv
	rm -rf $(DIST_DIR) $(BUILD_DIR) *.spec __pycache__
	rm -rf *.egg-info
	rm -rf $(TEMP_DIR)
	rm -rf *.log

# 打包为wheel
build: clean
	python setup.py bdist_wheel

help:
	@echo "Available targets:"
	@echo "  all      : Build the binary (default)"
	@echo "  install  : Install dependencies"
	@echo "  build    : Build the wheel"
	@echo "  clean    : Remove build artifacts"
	@echo "  distclean: Clean everything and reinstall dependencies"
	@echo "  docker   : Build Docker image"

.PHONY: build clean help