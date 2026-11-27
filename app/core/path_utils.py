"""系统路径与发布源安全校验工具 (SEC-001 Path Traversal Guard)

纯领域层安全路径校验，杜绝相对路径逃逸、系统核心敏感目录遍历、磁盘根目录直连等安全风险。
零 Web 框架依赖，校验失败统一抛出标准 ValueError。
"""

import os
from pathlib import Path

from app.config import settings


def validate_safe_folder_path(folder_path: str) -> str:
    """本地文件夹路径安全边界防护 (SEC-001 Path Traversal Guard):
    防止通过 ../ 目录逃逸、空字节截断、系统根探测或符号链接遍历访问系统敏感目录。
    校验成功返回规范化的 real_path (str)，失败抛出 ValueError。
    """
    if not folder_path or not folder_path.strip():
        raise ValueError("文件夹路径不能为空")
    if "\0" in folder_path:
        raise ValueError("文件夹路径包含非法空字符")

    # 显式禁止相对路径逃逸 (..)
    normalized_parts = folder_path.replace("\\", "/").split("/")
    if ".." in normalized_parts:
        raise ValueError("路径安全校验失败: 检测到相对路径逃逸字符 (..)")

    try:
        real_path = os.path.realpath(os.path.abspath(folder_path.strip()))
    except Exception as e:
        raise ValueError(f"解析路径失败: {e}") from e

    if not os.path.exists(real_path) or not os.path.isdir(real_path):
        raise ValueError(f"本地文件夹路径不存在或不是有效目录: {folder_path}")

    # 1. 禁止直接扫描磁盘或文件系统根目录
    p = Path(real_path)
    if p.parent == p or real_path in ("/", "\\") or (len(real_path) <= 3 and real_path.endswith(":\\")):
        raise ValueError("禁止直接将磁盘根目录作为发布源")

    # 2. 拦截操作系统核心敏感目录
    norm_slash = real_path.replace("\\", "/").lower()
    blocked_system_prefixes = (
        "c:/windows",
        "c:/program files",
        "c:/program files (x86)",
        "c:/programdata",
        "/etc",
        "/proc",
        "/sys",
        "/dev",
        "/root",
        "/boot",
        "/var/run",
        "/run",
    )
    for bp in blocked_system_prefixes:
        if norm_slash == bp or norm_slash.startswith(bp + "/"):
            raise ValueError("受限的系统目录，禁止作为发布源扫描")

    # 提前计算系统工程根目录与数据目录规范路径
    try:
        base_real = os.path.realpath(os.path.abspath(settings.BASE_DIR)).replace("\\", "/").lower()
    except Exception:
        base_real = ""
    try:
        data_real = os.path.realpath(os.path.abspath(settings.DATA_DIR)).replace("\\", "/").lower()
    except Exception:
        data_real = ""

    # 3. 拦截直接暴露服务工作根目录 (防止扫描 .session 和 .env) 或核心数据库目录
    if base_real and norm_slash == base_real:
        raise ValueError("禁止直接将系统工程根目录作为发布源扫描 (请指定其 downloads 或特定子目录)")
    if data_real and (norm_slash == data_real or norm_slash.startswith(data_real + "/")):
        raise ValueError("禁止扫描内部数据库与运行数据目录")

    # 4. 拦截高危敏感配置与凭据目录
    path_segments = set(norm_slash.split("/"))
    blocked_segments = {".ssh", ".gnupg", ".aws", ".git"}
    if ".gemini" in path_segments:
        # 白名单校验：仅在 DEBUG 模式放行，或属于工作区根目录下的合法子路径且工作区相对路径内未嵌套 .gemini
        is_safe_workspace_subpath = bool(
            base_real
            and norm_slash.startswith(base_real + "/")
            and ".gemini" not in norm_slash[len(base_real) :].split("/")
        )
        if not (getattr(settings, "DEBUG", False) or is_safe_workspace_subpath):
            blocked_segments.add(".gemini")

    if path_segments & blocked_segments:
        raise ValueError("检测到敏感凭据或配置目录，已安全拦截")

    return real_path
