"""任务类型策略实现包"""

from app.services.task_strategies.download_link_strategy import DownloadLinkStrategy
from app.services.task_strategies.download_range_strategy import DownloadRangeStrategy
from app.services.task_strategies.forward_channel_strategy import ForwardChannelStrategy
from app.services.task_strategies.pikpak_publish_strategy import PikPakPublishStrategy
from app.services.task_strategies.publish_folder_strategy import PublishFolderStrategy

__all__ = [
    "DownloadLinkStrategy",
    "DownloadRangeStrategy",
    "ForwardChannelStrategy",
    "PikPakPublishStrategy",
    "PublishFolderStrategy",
]
