# E2E live harness (`tests/e2e_live/`)

真账号 live-API E2E 层（设计文档：`docs/testing/E2E_GUIDE.md` §0~§3，）：
真服务器进程 + scratch `DATA_DIR`（完整克隆）+ 断言三通道（REST / DB 只读直查 / Telethon 只读拉取）。
**默认 skip，永不进 CI**；本目录所有环境变量只在包内经 `os.environ` 读取，不进 `app/config.py`。

## 手动触发流程

1. **先停真实服务**（正在运行的 tg_service）——harness 与真实服务共用同一 Telegram
   session，双进程并存会引发会话冲突（计划书 R-1）；默认端口仍被占用时 preflight 会自动
   skip 并提示。
2. 设置开关后执行（CMD）：

   ```bat
   set E2E_LIVE=1
   uv run pytest tests/e2e_live -q --tb=short
   uv run pytest tests/e2e_live/test_00_preflight.py -q
   ```

3. 结束后默认自动清理：scratch 目录（`tests/.tmp/e2e_live_*`）、有本轮发送结果或任务回执的频道消息
   （清理账本回收）、本次任务行（经 `DELETE /api/tasks/{task_id}`）。
4. 重启真实服务。

## 环境变量（全部 harness 私有）

| 变量 | 语义 | 缺省 |
|---|---|---|
| `E2E_LIVE` | 总开关，未设置整包收集即 skip | 未设 |
| `E2E_LIVE_BASE_URL` | 外部已起服务器地址（attach 模式会跳过依赖服务数据库的用例，包括重启和删除账号） | 未设 = harness 自起 |
| `E2E_LIVE_DATA_DIR` | 本轮独占的新目录，路径须尚不存在且位于克隆源之外 | `tests/.tmp/e2e_live_<UUID>` |
| `E2E_LIVE_SOURCE_DATA_DIR` | 克隆源 | 仓库 `data/` |
| `E2E_LIVE_TARGET_CHAT` | 测试频道 | 回退 `.env` 的 `TEST_CHANNEL_LINK` |
| `E2E_LIVE_FORWARD_TARGET` | E2E-LIVE-04 专用转发目标频道（禁止回退共享 `TRANSIT_CHAT_ID`） | 未设（缺省即 skip） |
| `E2E_LIVE_SAMPLE_DIR` | 样本根目录 | 回退 `.env` 的 `TEST_SAMPLE_FOLDER` |
| `E2E_LIVE_KEEP_SCRATCH` | 保留 scratch（改放项目持久缓存目录 `tests/tmp/e2e_live_*` 以幸存） | 未设 |
| `E2E_LIVE_KEEP_MSGS` | 保留频道消息；本轮任务照常清理 | 未设 |
| `E2E_LIVE_MAX_FILE_SIZE_MB` | 切片族低阈值注入·标准门限（§9.0 裁决） | 64 |
| `E2E_LIVE_PREMIUM_MAX_FILE_SIZE_MB` | 切片族低阈值注入·Premium 门限（§9.0 裁决） | 128 |
| `E2E_LIVE_PIKPAK_REMOTE_DIR` | PikPak 远端测试目录路径（切片族 E2E-LIVE-06~17 前置，未设逐项 skip） | 未设 |
| `E2E_LIVE_CAPTURE` | 启动套件时向服务器注入 `E2E_CAPTURE_MODE=true`（E2E-LIVE-17 前置） | 未设 |
| `E2E_LIVE_FORCE_PAGED_PROXY` | 启动套件时向服务器注入 `E2E_FORCE_PAGED_PROXY=true`（Tier 3 分页代理真链路验证，E2E-LIVE-23 前置） | 未设 |

## 切片族样本制备（E2E-LIVE-06 ~ 17 前置）

切片引擎仅在 PikPak 流式发布通道生效（`PublishFolderRequest` 无 `split_strategy`，
本地超限按 走 skip + 占位卡）。harness 以低阈值注入启动（§9.0 提速裁决，
上传是 live 链路绝对瓶颈）：

```bat
set E2E_LIVE_MAX_FILE_SIZE_MB=8
set E2E_LIVE_PREMIUM_MAX_FILE_SIZE_MB=12
```

**约 12.5 MiB 的样本按 ~7.8 MiB（非 Premium）/ ~11.7 MiB（Premium）切分线稳定切 2 卷**
（样本须 > Premium 门限 12 MiB，否则账号切回 Premium 时不会触发切分）。

1. 用 ffmpeg 在本地合成四份小样本（约 12.5 MiB 各一，不入仓库）：

   ```bat
   ffmpeg -y -f lavfi -i testsrc2=size=1280x720:rate=30:duration=14 -f lavfi -i sine=frequency=440:duration=14 -c:v libx264 -preset ultrafast -crf 23 -pix_fmt yuv420p -c:a aac -shortest -movflags +faststart e2e_F1s.mp4
   ffmpeg -y -i e2e_F1s.mp4 -c copy e2e_F2s.mp4
   ffmpeg -y -i e2e_F1s.mp4 -c:v copy -c:a ac3 e2e_F3s.mkv
   ffmpeg -y -f lavfi -i testsrc2=size=1280x720:rate=30:duration=20 -f lavfi -i sine=frequency=440:duration=20 -c:v libx265 -preset ultrafast -b:v 6M -pix_fmt yuv420p -c:a aac -shortest -movflags +faststart e2e_F6s.mp4
   ```

   样本规格（计划书附录 B 尺寸演进）：F1s = faststart MP4/H.264/AAC；F2s = 同 F1s 但
   rear-moov（`-c copy` 重封装默认 moov 后置）；F3s = MKV/H.264/AC3；F6s = HEVC/AAC
   MP4（双轨制备：有既有 HEVC 源时优先 `-c:v copy` 截段）。
   旧 96 MiB 版本样本已废（低阈值下会切出 13 卷，破坏 [1/2][2/2] 断言），
   **重传远端时必须删除旧文件**。

2. 将四份文件经 PikPak 客户端上传到专用测试目录（不要与其他文件混放），
   并把目录路径设入 `E2E_LIVE_PIKPAK_REMOTE_DIR`（如 `测试/e2e_live`，支持多级）。
   用例以 `auto_cleanup_mode=none` 发布，网盘文件保持原状可反复执行。
   split/zero-disk 提交统一携带 `upload_concurrency=8 × upload_in_flight_per_connection=2`
   注入（默认 2×1 在低速网络下 ~0.2 MB/s 是 live 链路绝对瓶颈）。
3. **E2E-LIVE-17（capture 取证联动）** 额外需要 `set E2E_LIVE_CAPTURE=1` 再启动套件：
   服务器进程注入 `E2E_CAPTURE_MODE=true`，为发布 item 留存 E1 源快照 + E2 分卷产物 +
   E3 TG 镜像三份证据，**磁盘成本 ≈3× 源体积/item**（96 MiB 样本约 290 MiB），
   随 scratch 目录一并回收。
4. **E2E-LIVE-19（结构化归档）** 需要 `剧集A/第一季/` 子目录结构，内含任一**小于当前
   `E2E_LIVE_MAX_FILE_SIZE_MB` 阈值**（默认 64 MiB，提速推荐 8 MiB）的视频文件（超阈值会被过滤跳过），例如：

   ```bat
   ffmpeg -y -f lavfi -i testsrc2=size=1280x720:rate=30:duration=10 -f lavfi -i sine=frequency=440:duration=10 -c:v libx264 -preset ultrafast -b:v 4M -pix_fmt yuv420p -c:a aac -shortest e2e_S1.mp4
   ```

   上传到 `剧集A/第一季/` 下。注意：该用例以 `auto_cleanup_mode=archive` 发布，
   文件将被移入**网盘根级 [已发布归档]/剧集A/第一季/**（产品标准归档落点，约 5 MiB
   残留可自行清理）；发布防重意味着同名同体积文件重跑会被跳过，重跑须更换文件名。

**V3 本地用例（E2E-LIVE-14 ~ 16）无需 PikPak 预置**：样本由 harness 以 ffmpeg 自动
制备（比特率定容，首次运行约 1-2 分钟，持久缓存在 `tests/tmp/e2e_live_samples`，
`E2E_LIVE_LOCAL_SAMPLES` 可覆盖），仅需 ffmpeg 在 PATH。

## 封面与准入样本制备（E2E-LIVE-24 ~ 27 前置）

- 本地封面/准入样本由 harness 以 ffmpeg 自动制备（幂等缓存于样本根目录）：
  `thumb_bright.mp4`（正常内容，E2E-LIVE-24 封面断言）、`thumb_black.mp4`（全黑片源，
  E2E-LIVE-25 近黑帧回退判别）、`admission_fake.ts`（TypeScript 文本伪装）、
  `admission_real.ts`（真 MPEG-TS）、`admission_clip.m4v`（MP4 内容挂 `.m4v` 后缀，
  三者共属 E2E-LIVE-26 本地准入族）。
- **E2E-LIVE-27（PikPak 远端准入）为 skip-if-absent**：需在 `E2E_LIVE_PIKPAK_REMOTE_DIR`
  远端测试目录预置 `*fake*.ts`（文本伪装件，**至少 816 字节**即 `MPEG_TS_MIN_REJECTION_BYTES = 204 × 4`，建议 ≥ 2 KiB；过小样本会触发远端校验 fail-open 而无法验证拦截）与真 `.ts`（`ffmpeg -f mpegts`
  产物）各一，命名含 `fake` 者按无效件断言；任一缺失该用例自动 skip。
- 封面断言口径：经 Telethon 只读缩略图下载（`thumb=-1`）+ Pillow 尺寸检查验证封面结构与归一化规格（`E2E-LIVE-24` 额外断言正常内容均值 > 40）。Telegram 会为带视频属性的消息服务端自生成预览，下载通道无法区分发送侧回退封面与平台黑帧，因此全黑片源与原地回填路径（`E2E-LIVE-25`）在 live 层以 `thumbs` 结构与规格断言为主，近黑回退语义由离线 `test_thumbnail_workflow` 锁定。

## 纪律

- 真实发送一律**串行**：套件内检测到 `PYTEST_XDIST_WORKER` 即整包 skip；
- 沿用应用自身节流（`send_delay_seconds` 走默认值，不调小）；
- **FloodWait 是预期产物**：`harness/tg_reader.py` 内置退避重试（`seconds + 1`，上限 3 次），
  触发型用例允许 SKIP，不算环境故障；
- 专用测试频道（`TEST_CHANNEL_LINK`）+ 备用账号，绝不触碰真实内容频道；
- 单日执行频次自控，出现风控征兆立即停跑。

## 故障排查

| 现象 | 处置 |
|---|---|
| 收集即 skip | `E2E_LIVE` 未设置（预期，CI 形态） |
| preflight skip：默认端口有服务存活 | 先停真实服务（R-1 守卫按设计生效） |
| 服务器未就绪超时 | 查 scratch 内 `server_stdout.log` 尾部（夹具异常信息已附带） |
| scratch 目录在哪 | KEEP 开启时在 `tests/tmp/e2e_live_*`；否则 `tests/.tmp/`（下一个 pytest 会话 sessionstart 自动清仓兜底） |
| scratch 克隆失败 | 确认 `data/` 存在且无进程占用库文件（真实服务已停） |
