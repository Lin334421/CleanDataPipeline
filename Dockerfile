# 使用官方 Python 镜像作为基础镜像
FROM python:3.10-slim

# 设置工作目录
WORKDIR /src

# 将当前目录内容复制到工作目录中
COPY . /src

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt

# 暴露端口（如果你的应用需要，例如 Flask 默认的5000端口）

# 定义容器启动时运行的命令
CMD ["python", "clean_gha.py"]
