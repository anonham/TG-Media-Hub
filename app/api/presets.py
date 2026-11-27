from fastapi import APIRouter, HTTPException, Query

from app.models.common import StatusOkResponse
from app.models.publish import PresetCreateRequest, PresetInspectRequest, PresetInspectResponse, PresetResponse
from app.services.channel_inspector import ChannelInspector
from app.services.preset_service import PresetService

router = APIRouter()


@router.get("", response_model=list[PresetResponse])
async def get_presets(type: str | None = Query(None, description="按预设类型过滤: 'target' | 'source'")):
    return PresetService.get_all(preset_type=type)


@router.post("/inspect", response_model=PresetInspectResponse)
async def inspect_channel(req: PresetInspectRequest):
    try:
        data = await ChannelInspector.inspect_channel(
            channel_identifier=req.channel_id, preset_type=req.preset_type, account_id=req.account_id
        )
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"频道权限探针检测失败: {e}") from e


@router.post("", response_model=PresetResponse)
async def add_preset(req: PresetCreateRequest):
    return PresetService.create(
        name=req.name, channel_id=req.channel_id, preset_type=req.preset_type, title=req.title or ""
    )


@router.delete("/{preset_id}", response_model=StatusOkResponse)
async def delete_preset(preset_id: int):
    PresetService.delete(preset_id)
    return {"status": "ok"}
