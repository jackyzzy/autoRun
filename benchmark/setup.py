from pathlib import Path

from setuptools import setup, find_packages

setup(
    name="benchmark",
    version="0.0.7",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            # 配置命令行入口，安装后可直接通过benchmark命令运行
            "benchmark = benchmark.main:main",
        ],
    },
    author="",
    author_email="",
    description="A benchmark tool",
    long_description=open("README.md").read() if Path("README.md").exists() else "",
    long_description_content_type="text/markdown",
    url="",  # 项目地址
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",  # 最低Python版本要求
    # 如果有依赖包，可以在这里列出
    install_requires=[
        "aiohttp",
        "tqdm",
        "numpy",
        "pandas",
        "openpyxl",
        "requests",
        "transformers"
    ],
)
