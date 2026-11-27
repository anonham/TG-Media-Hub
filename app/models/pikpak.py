from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, Field, model_validator


class PikPakStorageSummary(BaseModel):
    total_bytes: int | None = Field(None, description="网盘总空间 (字节; 待真实样本字段名校准)")
    used_bytes: int | None = Field(None, description="已用空间 (字节)")
    in_trash_bytes: int | None = Field(None, description="回收站占用 (字节)")


class PikPakAuthStatusResponse(BaseModel):
    logged_in: bool = Field(..., description="兼容镜像: state=='logged_in' 时 True, 否则 False (既有消费方零破坏)")
    state: Literal["logged_in", "not_bound", "unreachable"] = Field(
        "logged_in",
        description="三态: logged_in=已登录 | not_bound=未绑定/凭据失效 (恢复入口=设备码登录) "
        "| unreachable=暂时无法连接 PikPak (网络瞬断/CLI 异常, 等待自愈或重新检测)",
    )
    state_reason: str | None = Field(
        None, description="非 logged_in 态诊断摘要 (经 redact_text 脱敏, 不含凭据/直链; logged_in 时为 None)"
    )
    token_source: str = Field(
        "stored", description="凭据来源: env=环境变量接管 (前端禁扫码/禁编辑) | stored=CLI 已存凭据"
    )
    user_name: str | None = Field(None, description="账号昵称")
    email: str | None = Field(None, description="账号邮箱")
    vip_type: str | None = Field(None, description="VIP 类型摘要")
    storage: PikPakStorageSummary | None = Field(None, description="空间摘要")
    trash_duration_s: int | None = Field(None, description="回收站保留期 (秒)")

    @model_validator(mode="after")
    def _validate_logged_in_matches_state(self) -> "PikPakAuthStatusResponse":
        expected = self.state == "logged_in"
        if self.logged_in != expected:
            raise ValueError(f"logged_in ({self.logged_in}) 必须与 state ({self.state!r}) 一致")
        return self


class PikPakDeviceStartResponse(BaseModel):
    device_code: str = Field(..., description="设备码 (供轮询接口使用)")
    verification_uri: str = Field(..., description="授权页面 URL")
    user_code: str = Field(..., description="用户码 (在授权页输入)")
    expires_in: int | None = Field(None, description="设备码有效期 (秒)")
    interval: int = Field(5, description="建议轮询间隔 (秒)")


class PikPakDevicePollRequest(BaseModel):
    device_code: str = Field(..., description="设备码登录 start 返回的 device_code")
    interval_s: float = Field(5.0, ge=0.5, le=30, description="服务端轮询间隔 (秒)")
    max_wait_s: float = Field(30.0, ge=1.0, le=60, description="服务端本轮最长等待 (秒, 超时返回 timed_out)")


class PikPakDevicePollResponse(BaseModel):
    status: str = Field(
        ..., description="CLI 原样状态: success 或等待/过期/拒绝态 (非 success 值集待校准, 不写死 UI 文案)"
    )
    logged_in: bool = Field(False, description="是否已完成登录绑定")
    timed_out: bool = Field(False, description="本轮服务端等待是否超时 (可继续调用)")
    user_name: str | None = Field(None, description="登录成功后的账号昵称")
    email: str | None = Field(None, description="登录成功后的账号邮箱")


class PikPakFileItem(BaseModel):
    file_id: str = Field(..., description="PikPak 远端文件 ID (发布请求必填主键)")
    name: str = Field(..., description="文件名")
    is_dir: bool = Field(False, description="是否目录 (目录不可流式发布)")
    size_bytes: int = Field(0, description="文件大小 (字节)")
    audit: dict[str, Any] = Field(
        default_factory=dict, description="审核状态 {status,message,title}; 非通过条目 UI 标红"
    )
    thumbnail_link: str | None = Field(None, description="封面缩略图展示链接 (非下载直链)")
    duration_s: str | None = Field(None, description="视频时长 (秒, CLI 原始字符串; 存在性即视频判据)")
    width: str | None = Field(None, description="视频宽度 (像素, 原始字符串)")
    height: str | None = Field(None, description="视频高度 (像素, 原始字符串)")
    parent_id: str = Field("", description="父目录 ID (auto_cleanup 归档/删除定位用)")


class PikPakFileListResponse(BaseModel):
    items: list[PikPakFileItem] = Field(default_factory=list, description="当前页文件列表")
    next_page_token: str | None = Field(None, description="下一页 token (以实际返回为准, 透传分页)")


class PikPakInspectRequest(BaseModel):
    target_channel: str = Field(..., description="目标 Telegram 频道 ID/Username")
    account_id: int | None = Field(None, description="指定执行的 Userbot 账号 ID (可选)")
    remote_files: list[PikPakFileItem] = Field(default_factory=list, description="拟发布文件快照 (audit 汇总)")


class PikPakInspectResponse(BaseModel):
    accessible: bool = Field(..., description="目标频道对该账号是否可见可访问")
    has_write_permission: bool = Field(..., description="是否拥有发帖写权限")
    chat_id: str | None = Field(None, description="解析后的 Chat ID")
    channel_title: str | None = Field(None, description="频道标题")
    username: str | None = Field(None, description="频道用户名")
    permission_summary: str | None = Field(None, description="权限判定摘要")
    files_total: int = Field(0, description="拟发布文件总数")
    files_audit_blocked: int = Field(0, description="审核状态未通过数 (提交后将被跳过)")
    audit_blocked_names: list[str] = Field(default_factory=list, description="被拦截文件名 (UI 标红数据源)")
    total_size_bytes: int = Field(0, description="拟发布总字节数")
    warning: str | None = Field(None, description="警告提示")


class PikPakPublishRequest(BaseModel):
    remote_files: list[PikPakFileItem] = Field(
        ...,
        min_length=1,
        description="远端文件快照列表 (file_id 必填主键; 服务端无 search/get-by-id, 快照为执行期元数据唯一自举途径)",
    )
    target_channel: str = Field(..., description="目标 Telegram 频道 ID/Username (必填)")
    account_id: int | None = Field(None, description="指定执行的 Userbot 账号 ID (可选)")
    auto_cleanup_mode: Literal["none", "archive", "delete"] = Field(
        "archive",
        description="发布成功后网盘清理模式: none=保留原状; archive=移入已发布归档目录(默认); delete=移入回收站",
    )
    oversize_policy: Literal["skip_and_report", "split", "strict_fail"] = Field(
        "skip_and_report",
        description="超限文件处理策略: skip_and_report=跳过并在总结中报告(默认); split=存档保真分卷; strict_fail=严格报错中断",
    )
    split_strategy: Literal["vsse_with_fallback", "fmp4_universal", "raw"] | None = Field(
        None, description="超限流式切片引擎策略"
    )
    send_placeholder_card: bool = Field(
        True, description="是否在频道发送缺集占位说明卡 (skip_and_report 默认启用; 严禁写入 PublishHistory)"
    )
    force_republish: bool = Field(
        False,
        description="强制重传: True 时忽略已发布防重记录重复发送 (刷屏/风控风险, 前端需确认警示); 发布历史照常写入保持幂等",
    )
    priority: int | None = Field(10, description="任务优先级 (数值越大优先级越高, 默认 10)")
    send_delay_seconds: float = Field(3.0, ge=0.5, description="文件间发送风控延迟 (秒)")
    upload_concurrency: int | None = Field(
        None, ge=1, le=16, description="上传并发通道数 (1-16, 缺省 2=PIKPAK_UPLOAD_CONCURRENCY 稳健档)"
    )
    upload_in_flight_per_connection: int | None = Field(
        None,
        ge=1,
        le=4,
        validation_alias=AliasChoices("upload_in_flight_per_connection", "upload_in_flight"),
        description="单连接在途分块数规范契约 (1-4, 缺省 1=PIKPAK_UPLOAD_IN_FLIGHT)",
    )
    pikpak_prefetch_concurrency: int | None = Field(None, ge=1, le=16, description="预拉取并发通道数 (1-16, 推荐 6)")
    prefetch_concurrency: int | None = Field(None, ge=1, le=16, description="预拉取并发通道数别名 (1-16)")
    is_folder_publish: bool = Field(False, description="是否为文件夹形态发布 (由前端根据是否含目录选择计算)")
    folder_name: str | None = Field(None, description="文件夹标头名称 (用于频道总结卡等)")
    dir_tree: dict[str, str] | None = Field(
        default_factory=dict, description="目录树映射 {folder_id: rel_path} (用于结构化归档)"
    )
    album_mode: bool = Field(
        False,
        description="相册模式 (默认关闭): 同文件夹批次内图片合成相册 photo 形态发送; 单文件超过分卷阈值不入组 (走占位卡/跳过); 整组失败全员 failed 零入库",
    )

    @model_validator(mode="after")
    def _reject_directory_items(self):
        """护栏: 前端子树选择后载荷只应含叶子文件快照;
        目录条目属非法载荷, 422 拒绝。"""
        dir_names = [f.name or f.file_id for f in self.remote_files if f.is_dir]
        if dir_names:
            raise ValueError(f"remote_files 含目录条目 (应仅提交叶子文件快照): {', '.join(dir_names[:5])}")
        return self

    @model_validator(mode="after")
    def _reject_drive_root_selection(self):
        """护栏: 网盘根不允许被选中为发布对象 (前端禁用 + 后端 422 双重落地) ——
        dir_tree 空路径条目 = 网盘根被选。"""
        for fid, rel in (self.dir_tree or {}).items():
            if not str(rel or "").strip():
                raise ValueError(f"dir_tree 含空路径条目 (网盘根不允许被选中为发布对象): {fid}")
        return self
