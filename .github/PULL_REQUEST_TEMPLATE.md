## 变更说明

<!-- 简述本 PR 做了什么、为什么这样做、影响面有哪些 -->

## 关联 Issue

Closes #

## 变更类型

- [ ] Bug 修复 (`fix`)
- [ ] 新功能 (`feat`)
- [ ] 文档 (`docs`)
- [ ] 重构 / 测试 / 构建 (`refactor` / `test` / `chore`)

## 自查清单

- [ ] `uv run pytest -q -n auto -m "not benchmark"` 全量通过（与 CI 门禁一致）
- [ ] 新增功能附带测试用例
- [ ] 前端改动已通过 `pnpm run build` (产物输出 `../static`)
- [ ] API 变更已同步 README「API 接口索引」, 环境变量变更已同步「环境变量配置说明」
- [ ] 文档中新增的可验证声明 (默认值/路径/上限) 可在代码中找到出处
- [ ] 无新增硬编码凭据 (API ID/Hash、Bot Token、手机号、`.session`)
- [ ] SQL 查询全部参数绑定, 无字符串拼接
- [ ] Commit 信息遵循 Conventional Commits
