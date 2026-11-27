import { defineConfig } from '@hey-api/openapi-ts'

// 契约唯一真源: 后端 OpenAPI 快照 (tests/api/openapi_snapshot.json, 由 test_openapi_snapshot.py 锁定)。
// 重新生成: 后端快照更新后执行 `pnpm run openapi-ts`, 产物 api/gen/ 严禁手工编辑。
export default defineConfig({
  input: '../tests/api/openapi_snapshot.json',
  output: 'src/api/gen',
  plugins: ['@hey-api/client-axios'],
})
