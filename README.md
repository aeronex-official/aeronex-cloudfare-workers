# AERONEX Cloudflare Workers

本仓库用于维护 AERONEX 库存查询系统的 Cloudflare Worker。

## 生产环境

- Worker 名称：`tools-inventory`
- 生产地址：`https://tools-inventory.magic-ying.workers.dev`
- 主入口文件：`src/index.js`
- 部署配置：`wrangler.toml`
- 自动部署：GitHub Actions，`.github/workflows/deploy.yml`
- 默认分支：`main`

## 当前状态

### 2026-06-10

已完成 Worker v2.6.0 部署。

- Commit：`ff2ce13 Add public inventory search APIs`
- Worker 健康检查：`version: 2.6.0`
- 新增公开只读 API：
  - `GET /api/public/products?limit=500`
  - `GET /api/public/search?ean=...`
  - `GET /api/public/search?keyword=...`
- 目的：让公开库存查询页可以在不暴露 `X-Admin-Password` 的情况下查询库存。
- 验证结果：
  - `/api/public/products?limit=5` 可返回产品数据。
  - `/api/public/search?keyword=Matrice` 可返回库存记录。
  - `/api/public/search` 不带查询参数时返回 `400`，符合预期，避免全量拉取。

已完成 Cloudflare Pages 前端部署。

- Pages 项目：`aeronex-inventory-search`
- 生产域名：`https://tools-inventory-search.aeronex.ae`
- Deployment ID：`03f86b4c-a292-41b1-97a7-d6ded4dffab3`
- Deployment URL：`https://03f86b4c.aeronex-inventory-search.pages.dev`
- 来源文件：2026-06-10 从 Genspark 下载的最新导出包。
- 本次仅部署前端静态文件：
  - `index.html`
  - `admin.html`
  - `dashboard.html`
  - `migration.html`
  - `css/`
  - `js/`
  - `images/`
- 本次已排除不应公开部署的后端、SQL 和文档目录：
  - `github_repo/`
  - `lark_bot_repo/`
  - `sql/`
  - `README.md`
  - `KINGDEE-INTEGRATION-GUIDE.md`
- 验证结果：
  - `js/query.js` 已改为调用 `/api/public/products` 和 `/api/public/search`。
  - `js/admin.js` 已移除旧的硬编码 Admin 密码。
  - 使用 cache-busting 检查后，确认 `github_repo/index.js` 和 `README.md` 不再返回旧的公开文件内容。

### 2026-06-11

已完成线上前端回归验证。

- 验证范围：
  - Cloudflare Pages 自定义域名：`https://tools-inventory-search.aeronex.ae`
  - Cloudflare Worker public API：`https://tools-inventory.magic-ying.workers.dev`
- 验证结果：
  - 查询页首页返回 `200`，可正常加载。
  - `js/query.js` 返回 `200`，并确认调用 `/api/public/products` 与 `/api/public/search`。
  - `js/query.js` 未再包含旧的 `ADMIN_PASSWORD` 或 `Dji@2026`。
  - `/api/public/products?limit=2` 可返回 `{ data: [...] }` 格式的产品列表。
  - `/api/public/search?keyword=Matrice` 可返回 `{ data: [...] }` 格式的库存记录。
  - `github_repo/index.js` 与 `README.md` 在 Pages 域名下不再暴露源文件内容，而是回落到前端入口页。
- 已知限制：
  - 本机内置浏览器自动化检查受 Windows sandbox 限制中断，本阶段使用线上 HTTP、静态资源与 API 响应完成回归验证。

已完成 Supabase public API 暴露边界只读复核。

- Worker v2.6.0 public API：
  - `/api/public/products`
  - `/api/public/search`
- 当前 public API 只查询 Supabase `inventory` 表。
- 返回字段限于 `ean`、`model`、`warehouse`、`available_qty`。
- `/api/public/products` 限制 `limit <= 500`。
- `/api/public/search` 限制 `limit=100`，且必须提供 `ean` 或 `keyword`。
- Supabase 只读盘点发现：
  - `inventory` 对 `anon` 可见 316 行，符合公开查询页需求。
  - `inventory_snapshots` 与 `sales_records` 也存在 anon 可读数据，超出 Worker public API 当前设计范围，建议后续通过数据库迁移收紧。
  - `public.rls_auto_enable()` 为 `SECURITY DEFINER` 函数且 anon/authenticated 可执行，建议后续撤销公开执行权限。

已执行 Supabase 安全收紧迁移。

- 迁移记录：`aeronex-inventory-sync-/supabase/migrations/20260611_security_tightening.sql`
- 已撤销 `anon` / `authenticated` 在 public schema 所有表上的非 SELECT 权限。
- 已撤销 `public.rls_auto_enable()` 对 `public`、`anon`、`authenticated` 的执行权限。
- 已验证：
  - `public.rls_auto_enable()` 当前仅 `postgres` 与 `service_role` 可执行。
  - Worker public API `/api/public/products` 与 `/api/public/search` 正常。
  - Kingdee API `GET /api/inventory` 与 `POST /api/inventory/batch` 仍返回 `code=0`。
- 保留项：
  - `inventory_snapshots` 与 `sales_records` 暂时仍保留 `anon SELECT`，因为当前 `dashboard.html` 直接读取这两张表。
  - 后续应先将 Dashboard 改为通过 Worker 后端读取，再移除这两张表的 anon read policy。

### 2026-06-12

已完成 Worker v2.6.1 金蝶 B2B API warehouse 大小写兼容修复。

- 修改文件：`src/index.js`
- 修复范围：
  - `GET /api/inventory?ean=...`
  - `POST /api/inventory/batch`
- 修复内容：
  - 原逻辑使用 `includes('Dubai')` / `includes('Saudi')` 做大小写敏感匹配。
  - 当前 Supabase `inventory.warehouse` 实际为小写 `dubai` / `saudi` / `hk`。
  - 已改为 `(row.warehouse || '').toLowerCase()` 后再匹配 `dubai` / `saudi`。
- 本地验证：
  - `node --check src/index.js` 通过。
  - 小写 `dubai` / `saudi` 合并数量测试通过。

## 必须遵守的操作规则

每完成一个实现、部署、数据库迁移或验证阶段，都必须先更新 README，然后才能认为该阶段完成。

README 更新要求：

- 必须使用中文。
- 必须同时更新 GitHub 仓库中的 README。
- 必须同时更新本地项目文件夹中的 README。
- 如果某个阶段只影响一个子系统，也要在对应仓库 README 和本地总项目 README 中同步记录状态。

每次 README 更新应包含：

- 日期。
- 已完成的阶段。
- 变更的文件或系统。
- 部署目标，如有。
- 已完成的验证。
- 已知风险或后续事项。

## 已知后续事项

- 前端 Cloudflare Pages 目前还没有独立 GitHub 仓库。本次前端是从本地整理后的静态目录直接上传到 Cloudflare Pages。
- 建议后续将前端 Pages 源码纳入 GitHub，方便版本管理和可重复部署。
- 需要复查 Admin 前端登录状态逻辑：`js/admin.js` 只把登录状态存在 `sessionStorage`，密码只保存在内存中。页面刷新后，可能出现界面看似已登录，但 API 请求因内存密码丢失而失败的情况。
- 建议轮换旧 Admin 密码，因为旧密码曾经被硬编码在前端 JavaScript 中。
