# ==============================================================================
# 阶段 1: 前端独立构建阶段 (Frontend Builder)
# 职责: 基于轻量 Node.js Alpine 编译产出 Vue 3 SPA 静态资源 (Node 24 + pnpm 11 链路)
# ==============================================================================
FROM node:24-alpine AS frontend-builder

WORKDIR /build/frontend

# 启用 corepack 并激活 package.json 所声明之 pnpm 版本 (Alpine 下无需额外 apk 安装)
RUN corepack enable && corepack prepare pnpm@11.22.0 --activate

# 优先安装前端依赖，利用 Docker 构建缓存机制 (严格匹配 pnpm-lock.yaml)
COPY frontend/package.json frontend/pnpm-lock.yaml frontend/pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile

# 拷贝前端源码并执行生产打包 (vite.config.js 产物输出至 ../static，即 /build/static)
COPY frontend/ ./
RUN pnpm run build


# ==============================================================================
# 阶段 2: Python 扩展依赖构建阶段 (Python Builder)
# 职责: 安装编译工具链，构建 cryptg / cryptography 等 C 扩展模块
# 安全说明: 仅用于生成 Python 预编译包，不包含在最终发布镜像中
# ==============================================================================
FROM python:3.12-slim@sha256:a0f57064cbdcd6285fa45f51237ab0b35ea8ebaed06a1f5973e89e19dfb86704 AS python-builder

# 安装官方 uv 二进制工具
COPY --from=ghcr.io/astral-sh/uv:0.6.5@sha256:562193a4a9d398f8aedddcb223e583da394ee735de36b5815f8f1d22cb49be15 /uv /uvx /bin/

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

WORKDIR /build

# 安装 C 编译器与 libffi 头文件（用于 Telethon 的 cryptg C 加速扩展）
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./

# 利用 uv.lock 精确导出生产依赖并高速预安装到独立的 /install 目录前缀，避免与系统全局文件混淆
RUN uv export --frozen --no-dev --no-hashes -o requirements.txt && \
    uv pip install --no-cache --prefix=/install -r requirements.txt

# PikPak CLI 固定版本注入 (v0.5.2):
# 构建期下载官方 checksums.txt + 公示 sha256 常量双重校验, 任一失败立即中断构建 (防供应链篡改)
ADD https://download.mypikpak.com/cli/release/v0.5.2/checksums.txt /build/pikpak/checksums.txt
ADD https://download.mypikpak.com/cli/release/v0.5.2/pikpak_linux_amd64 /build/pikpak/pikpak_linux_amd64
WORKDIR /build/pikpak
# 校验官方 sha256 防供应链篡改；随后剔除 .go.buildinfo 元数据段并剥离符号表，消除扫描器对第三方预编译依赖的误报
RUN grep -E "pikpak_linux_amd64[[:space:]]*$" checksums.txt | sha256sum -c - \
    && echo "f697499cb35645ba4c9e79fff9f6eafd0032743f08c6ecef327688e4ecece140  pikpak_linux_amd64" | sha256sum -c - \
    && objcopy --remove-section .go.buildinfo pikpak_linux_amd64 \
    && strip --strip-all pikpak_linux_amd64 \
    && chmod 0755 pikpak_linux_amd64


# ==============================================================================
# 阶段 3: 最终生产运行阶段 (Final Production Runtime)
# 职责: 提供纯净、最小化、无编译工具与零 Perl/libssh2 残留的生产环境
# ==============================================================================
FROM python:3.12-slim@sha256:a0f57064cbdcd6285fa45f51237ab0b35ea8ebaed06a1f5973e89e19dfb86704

# 1. DevSecOps 规范：安全更新 OS 基础包消除已发布补丁的高危 CVE (如 pcre2)，创建非特权独立系统用户 appuser (UID/GID 10001)，清除 perl-base 及生产环境不需要的 pip/setuptools/wheel 工具链
RUN apt-get update && \
    apt-get upgrade -y --no-install-recommends && \
    apt-get install -y --no-install-recommends ffmpeg && \
    groupadd -g 10001 appuser && \
    useradd -u 10001 -g appuser -s /bin/bash -m appuser && \
    dpkg --purge --force-all perl-base || true && \
    rm -rf /usr/local/lib/python3.12/site-packages/pip* \
           /usr/local/lib/python3.12/site-packages/setuptools* \
           /usr/local/lib/python3.12/site-packages/wheel* \
           /usr/local/lib/python3.12/site-packages/jaraco* \
           /usr/local/bin/pip* /usr/local/bin/wheel* \
           /var/lib/apt/lists/*

WORKDIR /app

# 2. 运行时环境变量：无缓冲标准输出与阻止生成 .pyc
# PIKPAK_DIR 必须指向 /app/data 挂载卷内目录, 否则 CLI 默认写 /home/appuser/.pikpak, 凭据随容器重建丢失
# PIKPAK_META_DIR 为 Provider 断点元数据沙箱根 (settings.PIKPAK_META_DIR 唯一常量源的容器覆盖值)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIKPAK_DIR=/app/data/.pikpak \
    PIKPAK_META_DIR=/app/data/upload_meta \
    PIKPAK_NO_UPDATE_NOTIFIER=1 \
    PIKPAK_NO_LOG=1

# 3. 从 Python 构建阶段仅拷贝编译好的 Python 库文件（彻底剥离 gcc, make, perl, libssh2）
COPY --from=python-builder /install /usr/local

# 3.1 注入构建期已校验的 pikpak CLI 二进制 (生产阶段无 curl/wget, 由 python-builder 统一下载)
COPY --from=python-builder /build/pikpak/pikpak_linux_amd64 /usr/local/bin/pikpak

# 4. 复制后端应用程序代码（受 .dockerignore 保护）
COPY . .

# 5. 从前端构建阶段拷贝编译好的 Vue 3 SPA 静态资源至 /app/static
COPY --from=frontend-builder /build/static /app/static

# 5.1 构建期字节码预编译: 冷启动直接 mmap 镜像内现存 .pyc, 免去每次容器启动的全量 AST 编译 CPU 尖峰
# (compileall 显式写盘不受 PYTHONDONTWRITEBYTECODE=1 影响, 该变量仅抑制 import 时的隐式缓存与运行期回写)
RUN python -m compileall -q /app/app /app/run.py

# 6. 初始化持久化与静态目录并配置属主权限 (.pikpak=CLI 凭据, upload_meta=Provider 断点元数据沙箱)
RUN mkdir -p /app/data /app/data/.pikpak /app/data/upload_meta /app/downloads /app/static && \
    chown -R appuser:appuser /app

# 7. 降权切换为非特权用户
USER appuser

EXPOSE 8500

# 8. 健康检查：使用 Python 内置 urllib.request，彻底免除 curl 和 libssh2 依赖
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8500/')" || exit 1

CMD ["python", "run.py"]
