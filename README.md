# AERONEX Cloudflare Workers

Cloudflare Worker for the AERONEX inventory query system.

## Production

- Worker name: `tools-inventory`
- Production URL: `https://tools-inventory.magic-ying.workers.dev`
- Main entry: `src/index.js`
- Deploy config: `wrangler.toml`
- Auto deploy: GitHub Actions, `.github/workflows/deploy.yml`
- Default branch: `main`

## Current Status

### 2026-06-10

Completed Worker v2.6.0 deployment.

- Commit: `ff2ce13 Add public inventory search APIs`
- Worker health check: `version: 2.6.0`
- New public read-only APIs:
  - `GET /api/public/products?limit=500`
  - `GET /api/public/search?ean=...`
  - `GET /api/public/search?keyword=...`
- Purpose: allow the public query page to search inventory without exposing `X-Admin-Password` in frontend JavaScript.
- Verification:
  - `/api/public/products?limit=5` returned product data.
  - `/api/public/search?keyword=Matrice` returned inventory rows.
  - `/api/public/search` without query parameters returned `400`, as expected.

Completed Cloudflare Pages frontend deployment.

- Pages project: `aeronex-inventory-search`
- Production domain: `https://tools-inventory-search.aeronex.ae`
- Deployment ID: `03f86b4c-a292-41b1-97a7-d6ded4dffab3`
- Deployment URL: `https://03f86b4c.aeronex-inventory-search.pages.dev`
- Source package: latest Genspark export from 2026-06-10.
- Deployed frontend files only:
  - `index.html`
  - `admin.html`
  - `dashboard.html`
  - `migration.html`
  - `css/`
  - `js/`
  - `images/`
- Excluded backend, SQL, and documentation folders from Pages deployment:
  - `github_repo/`
  - `lark_bot_repo/`
  - `sql/`
  - `README.md`
  - `KINGDEE-INTEGRATION-GUIDE.md`
- Verification:
  - `js/query.js` now uses `/api/public/products` and `/api/public/search`.
  - `js/admin.js` no longer contains the hardcoded old admin password.
  - Cache-busted checks confirmed `github_repo/index.js` and `README.md` no longer return the old public file contents.

## Required Operating Rule

After every implementation, deployment, migration, or verification stage, update this README before considering the stage complete.

Each update should include:

- Date.
- Stage completed.
- Files or systems changed.
- Deployment target, if any.
- Verification performed.
- Known risks or follow-up items.

## Known Follow-Ups

- Frontend Pages currently does not have a dedicated GitHub repository. The latest deployment was done by direct Cloudflare Pages upload from a local curated static directory.
- Consider moving frontend Pages source into GitHub for version control and repeatable deployments.
- Review admin frontend session behavior: `js/admin.js` stores only login state in `sessionStorage` while the password is kept in memory. After page refresh, the page may appear logged in while API calls fail until the user logs in again.
- Consider rotating the old admin password because it was previously exposed in frontend JavaScript.
