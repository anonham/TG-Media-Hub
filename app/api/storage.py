import asyncio
import mimetypes
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse

from app.api.auth import verify_2fa_session, verify_api_key, verify_media_access
from app.models.storage import (
    BreakpointListResponse,
    CleanOrphansResponse,
    FileListResponse,
    StorageDeleteRequest,
    StorageDeleteResponse,
    StorageMetricsResponse,
    StorageOverviewResponse,
    StorageQuotaResponse,
    StorageQuotaUpdateRequest,
)
from app.services.storage_service import StorageService
from app.services.system_settings_service import SystemSettingsService

router = APIRouter()
manage_router = APIRouter(dependencies=[Depends(verify_api_key), Depends(verify_2fa_session)])
media_router = APIRouter(dependencies=[Depends(verify_media_access)])


@manage_router.get("/overview", response_model=StorageOverviewResponse)
async def storage_overview():
    """存储占用总览 (读扫描缓存; 首次访问或需要最新数据请先 POST /scan)"""
    return await asyncio.to_thread(StorageService.get_overview)


@manage_router.post("/scan", response_model=StorageOverviewResponse)
async def storage_scan():
    """重扫 downloads/ 并刷新登记缓存, 返回最新总览 (线程池执行, 不阻塞事件循环)"""
    return await asyncio.to_thread(StorageService.scan_and_register)


@manage_router.get("/files", response_model=FileListResponse | list[dict[str, Any]])
async def storage_files(
    kind: str | None = Query(
        None, description="分类过滤: video | image | audio | document | parts | final | part | meta | all"
    ),
    category: str | None = Query(None, description="分类过滤"),
    task_id: str | None = Query(None, description="按关联任务 ID 筛选"),
    query: str | None = Query(None, description="文件名/路径关键词搜索"),
    page: int | None = Query(None, ge=1, description="页码 (传入时开启分页响应)"),
    limit: int = Query(50, ge=1, le=2000, description="每页条数"),
    offset: int = Query(0, ge=0, description="偏移量"),
):
    """查询 downloads/ 及登记库中的媒体文件列表 (支持多维筛选与分页)"""
    filter_kind = category or kind or ""
    # 存量旧版接口兼容: 未传 page 且仅指定 kind in ('part', 'meta', 'final') 时返回 list_files
    if page is None and filter_kind in ("part", "meta", "final") and not task_id and not query:
        return await asyncio.to_thread(StorageService.list_files, filter_kind, min(max(limit, 1), 2000), max(offset, 0))

    p = page if page is not None else 1
    files, total = await asyncio.to_thread(
        StorageService.query_files,
        category=filter_kind,
        task_id=task_id or "",
        query=query or "",
        page=p,
        limit=limit,
        offset=None if page is not None else offset,
    )
    if page is not None:
        return FileListResponse(total=total, page=p, limit=limit, files=files)
    return files


def _resolve_media_file(file_path: str) -> tuple:
    """preview/download 共用前置三步: 安全校验 → 存在性 404 → mimetype 推断 (缺省 octet-stream)"""
    safe_path = StorageService.validate_safe_path(file_path)
    if not safe_path.exists() or not safe_path.is_file():
        raise HTTPException(status_code=404, detail="文件不存在")
    content_type, _ = mimetypes.guess_type(str(safe_path))
    return safe_path, content_type or "application/octet-stream"


@media_router.get("/preview/{file_path:path}")
async def storage_preview(file_path: str, range: str | None = Header(None)):
    """媒体在线流式播放与预览 (严格实现 HTTP 206 Partial Content 支持)"""
    safe_path, content_type = _resolve_media_file(file_path)

    file_size = safe_path.stat().st_size

    # 处理无 Range 请求 (200 OK)
    if not range:

        def iter_file(chunk_size=256 * 1024):
            with open(safe_path, "rb") as f:
                remaining = file_size
                while remaining > 0 and (chunk := f.read(min(chunk_size, remaining))):
                    remaining -= len(chunk)
                    yield chunk

        headers = {
            "Accept-Ranges": "bytes",
            "Content-Length": str(file_size),
            "Content-Type": content_type,
            "Content-Security-Policy": "sandbox",
            "X-Content-Type-Options": "nosniff",
        }
        return StreamingResponse(iter_file(), status_code=200, headers=headers, media_type=content_type)

    # 处理带有 Range 请求头的分块读取 (206 Partial Content)
    try:
        range_str = range.strip()
        if not range_str.startswith("bytes="):
            raise ValueError("Invalid range prefix")
        range_spec = range_str[6:].strip()
        parts = range_spec.split("-", 1)
        if len(parts) != 2:
            raise ValueError("Invalid range parts")

        start_str, end_str = parts[0].strip(), parts[1].strip()

        if start_str and end_str:
            start = int(start_str)
            end = int(end_str)
        elif start_str and not end_str:
            start = int(start_str)
            end = file_size - 1
        elif not start_str and end_str:
            suffix_len = int(end_str)
            start = max(0, file_size - suffix_len)
            end = file_size - 1
        else:
            raise ValueError("Empty range bounds")

        if start < 0 or start >= file_size or end < start:
            raise ValueError("Range out of bounds")

        end = min(end, file_size - 1)
        content_length = end - start + 1

    except Exception:
        raise HTTPException(
            status_code=416, detail="Requested Range Not Satisfiable", headers={"Content-Range": f"bytes */{file_size}"}
        ) from None

    def iter_range_chunks(start_byte: int, total_bytes: int, chunk_size: int = 256 * 1024):
        with open(safe_path, "rb") as f:
            f.seek(start_byte)
            remaining = total_bytes
            while remaining > 0:
                to_read = min(remaining, chunk_size)
                chunk = f.read(to_read)
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk

    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(content_length),
        "Content-Type": content_type,
        "Content-Security-Policy": "sandbox",
        "X-Content-Type-Options": "nosniff",
    }
    return StreamingResponse(
        iter_range_chunks(start, content_length), status_code=206, headers=headers, media_type=content_type
    )


@media_router.get("/download/{file_path:path}")
async def storage_download(file_path: str):
    """文件直接下载端点 (附带 Content-Disposition: attachment)"""
    safe_path, content_type = _resolve_media_file(file_path)

    return FileResponse(str(safe_path), filename=safe_path.name, media_type=content_type)


@manage_router.delete("/files", response_model=StorageDeleteResponse)
async def delete_storage_files(
    req: StorageDeleteRequest | None = None, path: str | None = Query(None, description="单个待删除文件路径")
):
    """单文件及批量删除接口 (具备严密安全路径守卫)"""
    targets = []
    if req and req.paths:
        targets.extend(req.paths)
    if path:
        targets.append(path)
    if not targets:
        raise HTTPException(status_code=400, detail="请提供待删除的文件路径 (paths 列表或 path 参数)")

    return await asyncio.to_thread(StorageService.delete_files, targets)


@manage_router.post("/delete", response_model=StorageDeleteResponse)
async def storage_delete(req: StorageDeleteRequest):
    """向后兼容删除接口"""
    return await asyncio.to_thread(StorageService.delete_files, req.paths)


@manage_router.get("/breakpoints", response_model=BreakpointListResponse)
async def storage_breakpoints():
    """获取所有 .part / .meta 断点碎片与配对状态"""
    data = await asyncio.to_thread(StorageService.get_breakpoints)
    return BreakpointListResponse(
        total=data["total"], orphan_count=data["orphan_count"], paired_count=data["paired_count"], items=data["items"]
    )


@manage_router.post("/clean-orphans", response_model=CleanOrphansResponse)
async def storage_clean_orphans():
    """一键扫描并清理无活跃任务关联的孤儿断点碎片"""
    return await asyncio.to_thread(StorageService.clean_orphans)


@manage_router.get("/metrics", response_model=StorageMetricsResponse)
async def storage_metrics():
    """获取宿主机磁盘容量、可用空间与项目占用量"""
    return await asyncio.to_thread(StorageService.get_metrics)


def _apply_storage_quota(req: StorageQuotaUpdateRequest) -> dict[str, Any]:
    """统一处理设置或取消下载存储配额上限"""
    SystemSettingsService.set(
        "max_storage_quota_gb",
        str(req.quota_gb),
        category="storage",
        description="下载存储配额上限 (GB, 0 表示不设上限按物理磁盘)",
    )
    return {
        "status": "ok",
        "max_storage_quota_gb": req.quota_gb,
        "message": f"存储配额已更新为 {req.quota_gb} GB"
        if req.quota_gb > 0
        else "已解除存储配额限制 (按物理磁盘实际可用空间)",
    }


@manage_router.put("/quota", operation_id="update_storage_quota_put", response_model=StorageQuotaResponse)
async def update_storage_quota_put(req: StorageQuotaUpdateRequest):
    """设置或取消下载存储配额上限 (PUT) (单位 GB, 0 表示不设上限按物理磁盘)"""
    return _apply_storage_quota(req)


@manage_router.post("/quota", operation_id="update_storage_quota_post", response_model=StorageQuotaResponse)
async def update_storage_quota_post(req: StorageQuotaUpdateRequest):
    """设置或取消下载存储配额上限 (POST) (单位 GB, 0 表示不设上限按物理磁盘)"""
    return _apply_storage_quota(req)


update_storage_quota = update_storage_quota_put


router.include_router(manage_router)
router.include_router(media_router)
