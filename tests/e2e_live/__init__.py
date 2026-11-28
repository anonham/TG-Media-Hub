"""E2E live harness 包 (docs/testing/E2E_GUIDE.md §2.2)。

真账号 live-API E2E 层: 真服务器进程 + scratch DATA_DIR + 断言三通道
(REST / DB 只读直查 / Telethon 只读拉取)。环境变量门控默认 skip, 不进 CI。
"""
