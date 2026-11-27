"""账号联动虚拟分卷纯逻辑。

- 联动对象是 Telegram Userbot 账号的 premium 状态 (task_is_premium), 与 PikPak 账号是否 VIP 无关;
- 分卷阈值一律由 app/config.py 既有三档常量 (MAX_FILE_SIZE_BYTES / PREMIUM_MAX_FILE_SIZE_BYTES /
  PROTOCOL_MAX_FILE_SIZE_BYTES) 运算派生, 禁止出现分卷阈值字面量 (1950/3900 等字节量级数值);
  派生公式为 已裁决的纯比率式: threshold = base * 975 // 1000 (整数乘除精确, 预留 2.5% 余量);
- 历史分卷为「存档保真备份, 非播放承诺」(V5.1 §二.3): 纯逻辑切片不引入 remux;
  自 起，主线默认采用 fmp4_universal (ZeroDiskFmp4StreamUploader) 承诺分卷 100% 即点即播与音画同步;
- 本模块为纯逻辑 (零 IO / 零媒体字节), 供 PublishPipeline / pikpak_publisher_service 消费。
"""

from dataclasses import dataclass

from app.config import settings

__all__ = [
    "PartSpec",
    "VirtualStreamSplitter",
    "derive_split_threshold",
    "format_part_caption",
]

# 裁决的安全余量比率 (分子/分母; 2.5% 预留, 恰得 2000 MiB→1950 MiB / 4000 MiB→3900 MiB 精确字节)
_THRESHOLD_NUMERATOR = 975
_THRESHOLD_DENOMINATOR = 1000


def derive_split_threshold(task_is_premium: bool) -> int:
    """按 Userbot premium 档位派生分卷阈值 (字节)。

    非 premium: settings.MAX_FILE_SIZE_BYTES (默认 2000 MiB (1.95 GiB) = 2,097,152,000 字节) → 1950 MiB (2,044,723,200 字节，余量 50 MiB);
    premium:    settings.PREMIUM_MAX_FILE_SIZE_BYTES (默认 4000 MiB (3.91 GiB) = 4,194,304,000 字节) → 3900 MiB (4,089,446,400 字节，余量 100 MiB)。
    结果必须不超过 settings.PROTOCOL_MAX_FILE_SIZE_BYTES 协议硬顶 (环境变量把基础档
    抬到协议帽之上时属配置错误, 直接抛 ValueError 拒绝)。
    """
    base = settings.PREMIUM_MAX_FILE_SIZE_BYTES if task_is_premium else settings.MAX_FILE_SIZE_BYTES
    threshold = base * _THRESHOLD_NUMERATOR // _THRESHOLD_DENOMINATOR
    protocol_cap = settings.PROTOCOL_MAX_FILE_SIZE_BYTES
    if threshold > protocol_cap:
        raise ValueError(
            f"分卷阈值 {threshold} 超过 Telegram 协议硬顶 {protocol_cap}, "
            "请检查 MAX_FILE_SIZE_MB / PREMIUM_MAX_FILE_SIZE_MB 配置"
        )
    return threshold


@dataclass(frozen=True)
class PartSpec:
    """虚拟分卷 Part 规格 (纯描述, 不持有任何字节)。

    offset: 本 Part 在虚拟源内的起始字节;
    length: 本 Part 字节长度 (恒 <= threshold);
    part_index: 1-based 序号 (直接供 caption "[i/n]" 与日志使用)。
    """

    offset: int
    length: int
    part_index: int


class VirtualStreamSplitter:
    """把 total_bytes 的虚拟源按 threshold 切成顺序 Part 列表 (零 IO, 纯边界计算)。"""

    @staticmethod
    def split(total_bytes: int, threshold: int) -> list[PartSpec]:
        """切分虚拟源。

        - total_bytes <= threshold → 单 Part (恰等于阈值含内);
        - total_bytes == 0 → [] (零字节文件: 调用方跳过引擎直传并告警);
        - total_bytes > threshold → ceil 除法切分, 前 n-1 个 length == threshold, 末 Part 为余量;
        - 防御: threshold > 协议帽 (Part 不可上传) / total_bytes < 0 / threshold <= 0 → ValueError。
        注: 不拒绝 total_bytes > 协议帽 —— PikPak 源文件可远大于 4096 MiB, 恰恰必须可切分;
        协议帽约束的是每个 Part 的上限 (即 threshold)。
        """
        if threshold <= 0:
            raise ValueError(f"分卷阈值必须为正整数, 实得 {threshold}")
        if threshold > settings.PROTOCOL_MAX_FILE_SIZE_BYTES:
            raise ValueError(
                f"分卷阈值 {threshold} 超过 Telegram 协议硬顶 {settings.PROTOCOL_MAX_FILE_SIZE_BYTES}, 该 Part 不可上传"
            )
        if total_bytes < 0:
            raise ValueError(f"total_bytes 不能为负数, 实得 {total_bytes}")
        if total_bytes == 0:
            return []

        parts: list[PartSpec] = []
        offset = 0
        part_index = 1
        while offset < total_bytes:
            length = min(threshold, total_bytes - offset)
            parts.append(PartSpec(offset=offset, length=length, part_index=part_index))
            offset += length
            part_index += 1
        return parts


def format_part_caption(original_name: str, part_index: int, total_parts: int) -> str:
    """Caption 规范: 多 Part 加 "[i/n] " 前缀; 单 Part (total_parts <= 1) 用原名无前缀。"""
    if total_parts <= 1:
        return original_name
    return f"[{part_index}/{total_parts}] {original_name}"
