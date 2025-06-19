FROM openwhisk/python3action:latest

# 安装编译依赖（duckdb 需要 C++ 编译器）
RUN apk add --no-cache gcc g++ musl-dev

# 升级 pip 以避免警告
RUN pip install --upgrade pip

# 安装额外的 Python 包
RUN pip install duckdb requests

# 清理编译依赖以减小镜像大小（可选）
RUN apk del gcc g++ musl-dev

# 注意：time、re、os、json 是 Python 标准库，无需额外安装