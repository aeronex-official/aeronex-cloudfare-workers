/**
 * AERONEX Lark Bot + Admin API - Cloudflare Worker
 * Version: 2.2.1
 * 功能：
 *   - 接收 Lark 消息，查询 Supabase 库存数据
 *   - /api/admin/* 提供库存管理 REST API（需 X-Admin-Password 验证）
 * Changelog:
 *   2.1.1 - 修复 setSession() 使用 UPSERT，解决多用户 session 丢失导致"查询已过期"的 Bug
 *   2.1.2 - 修复 searchByModel() encodeURIComponent 将 * 编码为 %2A，导致 ilike 通配符失效
 *           修复 mergeProducts() 改用 Map 替代普通 Object，避免纯数字 EAN key 被 V8 自动排序
 *   2.1.3 - 修复 setSession() UPSERT 未指定 on_conflict=open_id，导致冲突时以主键判断
 *           旧记录永远无法更新，session 停留在首次写入值，改为 DELETE + INSERT 彻底解决
 *   2.2.0 - 新增 Lark 一键导出库存功能：发送「导出/export」触发，自动生成 UTF-8 BOM CSV
 *           上传至 Lark Drive 后以文件消息发送；上传失败时降级为文字摘要+管理后台链接
 *           支持中英文双语触发词（导出/导出库存/export/export inventory 等8个关键词）
 *   2.2.1 - 修复导出文件无法在聊天窗口显示的问题：将上传 API 由 Drive API（返回 file_token）
 *           改为 IM Files API（/open-apis/im/v1/files，返回 file_key）
 *           Drive file_token 与 IM file_key 是不同凭证，只有 file_key 才能用于 IM 文件消息
 *   2.2.2 - 修复 handleExport 异常被静默吞掉无法排查根因的问题
 *           改为分步执行并将真实错误信息回复给用户（含步骤名+错误描述）
 *           方便在 Lark 聊天窗口直接看到失败原因，无需查看 Worker 日志
 */

const LARK_BASE_URL = 'https://open.larksuite.com';
const VERSION = '2.2.2';

// ============================================================
// CORS 工具
// ============================================================

function corsHeaders(origin) {
  return {
    'Access-Control-Allow-Origin': origin || '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, X-Admin-Password',
    'Access-Control-Max-Age': '86400'
  };
}

function jsonResponse(data, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...extraHeaders
    }
  });
}

// ============================================================
// 工具函数
// ============================================================

function getSupabaseHeaders(supabaseKey) {
  return {
    'apikey': supabaseKey,
    'Authorization': `Bearer ${supabaseKey}`,
    'Content-Type': 'application/json'
  };
}

async function getLarkToken(appId, appSecret) {
  const resp = await fetch(`${LARK_BASE_URL}/open-apis/auth/v3/tenant_access_token/internal`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ app_id: appId, app_secret: appSecret })
  });
  const data = await resp.json();
  return data.tenant_access_token || '';
}

function isEan(keyword) {
  return /^\d{8,14}$/.test(keyword.trim());
}

// ============================================================
// event_id 去重（存 Supabase）
// ============================================================

async function isEventProcessed(eventId, supabaseUrl, supabaseKey) {
  if (!eventId) return false;
  try {
    const resp = await fetch(
      `${supabaseUrl}/rest/v1/processed_events?event_id=eq.${eventId}&select=id`,
      { headers: getSupabaseHeaders(supabaseKey) }
    );
    const data = await resp.json();
    if (data && data.length > 0) return true;

    await fetch(`${supabaseUrl}/rest/v1/processed_events`, {
      method: 'POST',
      headers: { ...getSupabaseHeaders(supabaseKey), 'Prefer': 'return=minimal' },
      body: JSON.stringify({ event_id: eventId })
    });
    return false;
  } catch (e) {
    return false;
  }
}

// ============================================================
// 用户 Session（存 Supabase）
// ============================================================

async function getSession(openId, supabaseUrl, supabaseKey) {
  try {
    const resp = await fetch(
      `${supabaseUrl}/rest/v1/user_sessions?open_id=eq.${openId}&select=products,updated_at`,
      { headers: getSupabaseHeaders(supabaseKey) }
    );
    const data = await resp.json();
    if (!data || data.length === 0) return [];

    const updatedAt = new Date(data[0].updated_at).getTime();
    const now = Date.now();
    if (now - updatedAt > 5 * 60 * 1000) return [];

    return data[0].products || [];
  } catch (e) {
    return [];
  }
}

async function setSession(openId, products, supabaseUrl, supabaseKey) {
  // 策略：先 DELETE 该 open_id 的旧记录，再 INSERT 新记录
  // 原因：Supabase UPSERT（resolution=merge-duplicates）默认以主键 id 判断冲突，
  //       而非 open_id；指定 on_conflict=open_id 在 PostgREST v10 以下版本不稳定。
  //       DELETE + INSERT 是最可靠的"存在则替换"实现，且操作均在独立事务中完成。
  try {
    // Step 1：删除该用户的旧 session（若不存在则无影响）
    await fetch(
      `${supabaseUrl}/rest/v1/user_sessions?open_id=eq.${encodeURIComponent(openId)}`,
      {
        method: 'DELETE',
        headers: getSupabaseHeaders(supabaseKey)
      }
    );

    // Step 2：插入最新 session
    await fetch(`${supabaseUrl}/rest/v1/user_sessions`, {
      method: 'POST',
      headers: {
        ...getSupabaseHeaders(supabaseKey),
        'Prefer': 'return=minimal'
      },
      body: JSON.stringify({
        open_id: openId,
        products,
        updated_at: new Date().toISOString()
      })
    });
  } catch (e) {
    // session 写入失败不影响主流程
  }
}

// ============================================================
// 库存查询（查 Supabase）
// ============================================================

async function searchByEan(keyword, supabaseUrl, supabaseKey) {
  const resp = await fetch(
    `${supabaseUrl}/rest/v1/inventory?ean=eq.${encodeURIComponent(keyword)}&select=ean,model,warehouse,available_qty`,
    { headers: getSupabaseHeaders(supabaseKey) }
  );
  const rows = await resp.json();
  return mergeProducts(rows);
}

async function searchByModel(keyword, supabaseUrl, supabaseKey) {
  // 不能用 encodeURIComponent：它会把 ilike 通配符 * 编码成 %2A，导致模糊匹配失效
  // 只去除 PostgREST 语法中的敏感字符（逗号、括号），空格由 fetch 自动处理为 %20
  const safeKeyword = keyword.replace(/[,()]/g, '');
  const resp = await fetch(
    `${supabaseUrl}/rest/v1/inventory?model=ilike.*${safeKeyword}*&select=ean,model,warehouse,available_qty&limit=500`,
    { headers: getSupabaseHeaders(supabaseKey) }
  );
  const rows = await resp.json();
  const kw = keyword.toLowerCase();
  const filtered = rows.filter(r => r.model && r.model.toLowerCase().includes(kw));
  const products = mergeProducts(filtered);

  products.sort((a, b) => {
    const am = a.model.toLowerCase();
    const bm = b.model.toLowerCase();
    const score = m => m === kw ? 0 : m.startsWith(kw) ? 1 : 2;
    return score(am) - score(bm);
  });
  return products;
}

function mergeProducts(rows) {
  // 使用 Map 而非普通 Object：普通 Object 对纯数字 key（EAN）会被 V8 引擎按数值自动排序
  // 导致合并后产品顺序被打乱，与搜索结果列表顺序不一致，数字选择时返回错误产品
  // Map 严格保持 key 插入顺序，确保列表编号与 session 存储的产品一一对应
  const productsMap = new Map();
  for (const row of rows) {
    const ean = String(row.ean || '').trim();
    if (!ean) continue;
    if (!productsMap.has(ean)) {
      productsMap.set(ean, { ean, model: row.model || '', dubai_qty: null, saudi_qty: null });
    }
    const entry = productsMap.get(ean);
    const qty = row.available_qty ?? 0;
    if (row.warehouse && row.warehouse.includes('Dubai')) {
      entry.dubai_qty = qty;
    } else if (row.warehouse && row.warehouse.includes('Saudi')) {
      entry.saudi_qty = qty;
    }
  }
  return Array.from(productsMap.values());
}

// ============================================================
// 导出功能
// ============================================================

/**
 * 检测用户输入是否为导出指令
 * 支持中英文双语触发词
 */
function isExportCommand(keyword) {
  const kw = keyword.trim().toLowerCase();
  const triggers = [
    '导出', '导出库存', '导出清单', '导出报表',
    'export', 'export list', 'export inventory', 'export report'
  ];
  return triggers.includes(kw);
}

/**
 * 分页拉取 Supabase inventory 表全量数据
 * 每页 1000 条，直到返回行数 < pageSize 为止
 */
async function fetchAllInventory(supabaseUrl, supabaseKey) {
  const allRows = [];
  let offset = 0;
  const pageSize = 1000;
  while (true) {
    const resp = await fetch(
      `${supabaseUrl}/rest/v1/inventory?select=ean,model,warehouse,available_qty&order=model.asc&limit=${pageSize}&offset=${offset}`,
      { headers: getSupabaseHeaders(supabaseKey) }
    );
    const rows = await resp.json();
    if (!Array.isArray(rows) || rows.length === 0) break;
    allRows.push(...rows);
    if (rows.length < pageSize) break;
    offset += pageSize;
  }
  return allRows;
}

/**
 * 将库存原始数据（迪拜/沙特各一行）按 EAN 合并，生成 UTF-8 BOM CSV 字符串
 * BOM（\uFEFF）确保 Excel 直接打开时中文不乱码
 * 表头：EAN,产品型号/Model,迪拜库存/Dubai,沙特库存/Saudi,合计/Total,同步时间/Sync Time
 */
function buildCsvContent(rows) {
  const map = new Map();
  for (const row of rows) {
    const ean = String(row.ean || '').trim();
    if (!ean) continue;
    if (!map.has(ean)) {
      map.set(ean, { ean, model: row.model || '', dubai: null, saudi: null });
    }
    const entry = map.get(ean);
    const qty = row.available_qty ?? 0;
    if (row.warehouse && row.warehouse.includes('Dubai')) entry.dubai = qty;
    else if (row.warehouse && row.warehouse.includes('Saudi')) entry.saudi = qty;
  }

  const syncTime = new Date().toISOString().replace('T', ' ').slice(0, 16) + ' UTC';
  const BOM = '\uFEFF'; // UTF-8 BOM，让 Excel 直接打开中文不乱码
  const header = 'EAN,产品型号/Model,迪拜库存/Dubai,沙特库存/Saudi,合计/Total,同步时间/Sync Time';

  const dataLines = Array.from(map.values()).map(p => {
    const dubai = p.dubai ?? 0;
    const saudi = p.saudi ?? 0;
    const total = dubai + saudi;
    // 型号中若含逗号需加引号，避免 CSV 解析错误
    const model = p.model.includes(',') ? `"${p.model}"` : p.model;
    return `${p.ean},${model},${dubai},${saudi},${total},${syncTime}`;
  });

  return BOM + header + '\n' + dataLines.join('\n');
}

/**
 * 将 CSV 字符串上传至 Lark IM Files API
 * 使用 multipart/form-data 调用 /open-apis/im/v1/files
 * 成功返回 file_key（IM 消息专用凭证），失败返回 null
 *
 * ⚠️ 注意区分两个不同 API：
 *   - Drive API（drive/v1/files/upload_all）→ 返回 file_token（云文档凭证，不能用于 IM 消息）
 *   - IM Files API（im/v1/files）→ 返回 file_key（IM 消息专用，sendFileMessage 需要此值）
 * 必须使用 IM Files API 才能在聊天窗口中直接发送可下载的文件消息
 */
async function uploadFileToLark(csvContent, filename, token) {
  const encoder = new TextEncoder();
  const csvBytes = encoder.encode(csvContent);
  const blob = new Blob([csvBytes], { type: 'text/csv' });

  const formData = new FormData();
  formData.append('file_type', 'csv');    // IM 文件类型，对应 CSV 格式
  formData.append('file_name', filename); // 文件名（含 .csv 后缀）
  formData.append('file', blob, filename);

  const resp = await fetch(
    `${LARK_BASE_URL}/open-apis/im/v1/files`, // ← IM Files API，返回 file_key
    {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${token}` },
      // 不手动设置 Content-Type，让 fetch 自动设置正确的 multipart boundary
      body: formData
    }
  );
  const data = await resp.json();
  // 返回 file_key，后续 sendFileMessage 中以 { file_key: "..." } 格式发送
  return data?.data?.file_key || null;
}

/**
 * 发送文件消息（Lark file 类型消息）
 * 私聊发送给 open_id，群聊发送到 chat_id
 */
async function sendFileMessage(openId, fileToken, token, isGroup, chatId) {
  const url = `${LARK_BASE_URL}/open-apis/im/v1/messages`;
  const receiveIdType = (isGroup && chatId) ? 'chat_id' : 'open_id';
  const receiveId = (isGroup && chatId) ? chatId : openId;

  await fetch(`${url}?receive_id_type=${receiveIdType}`, {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      receive_id: receiveId,
      msg_type: 'file',
      content: JSON.stringify({ file_key: fileToken })
    })
  });
}

/**
 * 导出主流程编排：
 * 1. 立即回复"生成中"提示
 * 2. 拉取全量数据 → 生成 CSV → 上传 Lark Drive → 发送文件消息
 * 3. 上传失败时降级为文字摘要 + 管理后台链接
 */
async function handleExport(openId, token, isGroup, chatId, supabaseUrl, supabaseKey) {
  await sendReply(openId, '⏳ 正在生成库存报表，请稍候...', token, isGroup, chatId);

  // ── Step 1：拉取全量库存数据 ──
  let rows;
  try {
    rows = await fetchAllInventory(supabaseUrl, supabaseKey);
  } catch (e) {
    await sendReply(openId, `❌ [Step1] 读取数据库失败：${e.message}`, token, isGroup, chatId);
    return;
  }
  if (!rows || rows.length === 0) {
    await sendReply(openId, '❌ [Step1] 数据库返回空数据，请检查 Supabase inventory 表是否有数据', token, isGroup, chatId);
    return;
  }

  // ── Step 2：生成 CSV 内容 ──
  let csvContent, filename, skuCount, syncTime;
  try {
    csvContent = buildCsvContent(rows);
    const date = new Date().toISOString().slice(0, 10);
    filename = `AERONEX_Inventory_${date}.csv`;
    skuCount = (csvContent.match(/\n/g) || []).length - 1;
    syncTime = new Date().toISOString().replace('T', ' ').slice(0, 16) + ' UTC';
  } catch (e) {
    await sendReply(openId, `❌ [Step2] CSV 生成失败：${e.message}`, token, isGroup, chatId);
    return;
  }

  // ── Step 3：上传至 Lark IM Files API，获取 file_key ──
  let fileKey;
  try {
    fileKey = await uploadFileToLark(csvContent, filename, token);
  } catch (e) {
    await sendReply(openId,
      `❌ [Step3] 文件上传请求异常：${e.message}\n请检查 Lark 应用是否已开启 im:file:write 权限`,
      token, isGroup, chatId);
    return;
  }

  if (!fileKey) {
    // file_key 为空时，重新调用一次拿到具体错误码
    let errDetail = '未知错误';
    try {
      const encoder = new TextEncoder();
      const blob = new Blob([encoder.encode(csvContent)], { type: 'text/csv' });
      const fd = new FormData();
      fd.append('file_type', 'csv');
      fd.append('file_name', filename);
      fd.append('file', blob, filename);
      const r = await fetch(`${LARK_BASE_URL}/open-apis/im/v1/files`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}` },
        body: fd
      });
      const d = await r.json();
      errDetail = `Lark code=${d.code}，msg=${d.msg}`;
    } catch (e2) {
      errDetail = e2.message;
    }
    await sendReply(openId,
      `❌ [Step3] 文件上传失败：${errDetail}\n\n🔧 常见原因：\n1. Lark 应用未开启 im:file:write 权限\n2. tenant_access_token 获取失败\n\n可访问管理后台手动导出：\nhttps://tools-inventory-search.aeronex.ae/admin.html`,
      token, isGroup, chatId);
    return;
  }

  // ── Step 4：发送文字摘要 + 文件消息 ──
  try {
    await sendReply(
      openId,
      `✅ 库存报表已生成\n📦 共 ${skuCount} 个 SKU\n🗓 数据时间：${syncTime}`,
      token, isGroup, chatId
    );
    await sendFileMessage(openId, fileKey, token, isGroup, chatId);
  } catch (e) {
    await sendReply(openId, `❌ [Step4] 发送文件消息失败：${e.message}`, token, isGroup, chatId);
  }
}

// ============================================================
// 消息格式化
// ============================================================

function formatQty(qty) {
  if (qty === null || qty === undefined) return '—';
  if (qty > 0) return `✅ ${qty} 件`;
  if (qty < 0) return `⚠️ ${qty} 件`;
  return '❌ 无库存';
}

function formatProductDetail(p) {
  return [
    '━'.repeat(28),
    `📦 ${p.model}`,
    `EAN: ${p.ean}`,
    `🇦🇪 Dubai:  ${formatQty(p.dubai_qty)}`,
    `🇸🇦 Saudi:  ${formatQty(p.saudi_qty)}`,
    '━'.repeat(28)
  ].join('\n');
}

function formatSearchList(keyword, products) {
  const top10 = products.slice(0, 10);
  const lines = [
    `🔍 「${keyword}」找到 ${products.length} 个相关产品\n`,
    `📋 请输入编号查看库存详情：\n`,
    ...top10.map((p, i) => `${i + 1}. ${p.model}`),
    `\n💡 输入数字 1-${top10.length} 查看详情`
  ];
  return lines.join('\n');
}

// ============================================================
// 消息处理
// ============================================================

async function handleMessage(openId, keyword, supabaseUrl, supabaseKey) {
  keyword = keyword.trim();
  if (!keyword) return null;

  // ── 导出指令（优先级最高，在 EAN/数字选择/型号搜索之前判断）──
  if (isExportCommand(keyword)) {
    return '__EXPORT__'; // 特殊标记，由主入口调用 handleExport() 处理
  }

  if (/^\d{1,2}$/.test(keyword)) {
    const num = parseInt(keyword);
    const session = await getSession(openId, supabaseUrl, supabaseKey);
    if (session && session.length > 0) {
      if (num >= 1 && num <= session.length) {
        return formatProductDetail(session[num - 1]);
      }
      return `⚠️ 请输入 1-${session.length} 之间的数字`;
    }
    return '⚠️ 查询已过期，请重新输入产品名称或EAN码';
  }

  if (isEan(keyword)) {
    const products = await searchByEan(keyword, supabaseUrl, supabaseKey);
    if (!products.length) {
      return `❌ 未找到 EAN「${keyword}」\n\n请确认EAN码是否正确，或尝试输入型号关键词`;
    }
    await setSession(openId, products, supabaseUrl, supabaseKey);
    return formatProductDetail(products[0]);
  }

  const products = await searchByModel(keyword, supabaseUrl, supabaseKey);
  if (!products.length) {
    return `❌ 未找到与「${keyword}」相关的产品\n\n请尝试：\n• 输入完整 EAN 码（如：6937224106420）\n• 输入型号关键词（如：Zenmuse X7、Matrice 400）`;
  }

  if (products.length === 1) {
    await setSession(openId, products, supabaseUrl, supabaseKey);
    return formatProductDetail(products[0]);
  }

  await setSession(openId, products.slice(0, 10), supabaseUrl, supabaseKey);
  return formatSearchList(keyword, products);
}

// ============================================================
// 发送 Lark 消息
// ============================================================

async function sendReply(openId, text, token, isGroup, chatId) {
  const url = `${LARK_BASE_URL}/open-apis/im/v1/messages`;
  if (isGroup && chatId) {
    await fetch(`${url}?receive_id_type=chat_id`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ receive_id: chatId, msg_type: 'text', content: JSON.stringify({ text }) })
    });
  } else {
    await fetch(`${url}?receive_id_type=open_id`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ receive_id: openId, msg_type: 'text', content: JSON.stringify({ text }) })
    });
  }
}

// ============================================================
// Admin API 路由处理
// ============================================================

async function handleAdminRequest(request, url, env) {
  const origin = request.headers.get('Origin') || '*';
  const cors = corsHeaders(origin);

  // OPTIONS 预检
  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: cors });
  }

  // 验证 Admin 密码
  const adminPassword = request.headers.get('X-Admin-Password') || '';
  if (!env.ADMIN_PASSWORD || adminPassword !== env.ADMIN_PASSWORD) {
    return jsonResponse({ error: 'Unauthorized' }, 401, cors);
  }

  const supabaseUrl = env.SUPABASE_URL;
  const supabaseKey = env.SUPABASE_SECRET_KEY; // 使用 service_role key 进行写操作
  const headers = getSupabaseHeaders(supabaseKey);

  const pathname = url.pathname;
  const method = request.method;

  // ── GET /api/admin/inventory ── 分页列表
  if (method === 'GET' && pathname === '/api/admin/inventory') {
    const page = parseInt(url.searchParams.get('page') || '1');
    const limit = parseInt(url.searchParams.get('limit') || '500');
    const search = url.searchParams.get('search') || '';
    const warehouse = url.searchParams.get('warehouse') || '';
    const offset = (page - 1) * limit;

    let filter = '';
    if (search) {
      filter += `&or=(model.ilike.*${encodeURIComponent(search)}*,ean.ilike.*${encodeURIComponent(search)}*)`;
    }
    if (warehouse) {
      filter += `&warehouse=eq.${encodeURIComponent(warehouse)}`;
    }

    // 获取总数
    const countResp = await fetch(
      `${supabaseUrl}/rest/v1/inventory?select=id${filter}`,
      { headers: { ...headers, 'Prefer': 'count=exact', 'Range': '0-0' } }
    );
    const contentRange = countResp.headers.get('content-range') || '0-0/0';
    const total = parseInt(contentRange.split('/')[1] || '0');

    // 获取数据
    const dataResp = await fetch(
      `${supabaseUrl}/rest/v1/inventory?select=*${filter}&offset=${offset}&limit=${limit}&order=model.asc`,
      { headers }
    );
    const data = await dataResp.json();

    return jsonResponse({ data, total, page, limit }, 200, cors);
  }

  // ── GET /api/admin/inventory/:id ── 单条记录
  const singleMatch = pathname.match(/^\/api\/admin\/inventory\/([^/]+)$/);
  if (method === 'GET' && singleMatch) {
    const id = singleMatch[1];
    const resp = await fetch(
      `${supabaseUrl}/rest/v1/inventory?id=eq.${encodeURIComponent(id)}&select=*`,
      { headers }
    );
    const data = await resp.json();
    if (!data || data.length === 0) return jsonResponse({ error: 'Not found' }, 404, cors);
    return jsonResponse(data[0], 200, cors);
  }

  // ── POST /api/admin/inventory ── 新增记录
  if (method === 'POST' && pathname === '/api/admin/inventory') {
    const body = await request.json();
    const resp = await fetch(`${supabaseUrl}/rest/v1/inventory`, {
      method: 'POST',
      headers: { ...headers, 'Prefer': 'return=representation' },
      body: JSON.stringify(body)
    });
    const data = await resp.json();
    return jsonResponse(Array.isArray(data) ? data[0] : data, 201, cors);
  }

  // ── PUT /api/admin/inventory/:id ── 全量更新
  const putMatch = pathname.match(/^\/api\/admin\/inventory\/([^/]+)$/);
  if (method === 'PUT' && putMatch) {
    const id = putMatch[1];
    const body = await request.json();
    const resp = await fetch(`${supabaseUrl}/rest/v1/inventory?id=eq.${encodeURIComponent(id)}`, {
      method: 'PATCH',
      headers: { ...headers, 'Prefer': 'return=representation' },
      body: JSON.stringify(body)
    });
    const data = await resp.json();
    return jsonResponse(Array.isArray(data) ? data[0] : data, 200, cors);
  }

  // ── DELETE /api/admin/inventory/:id ── 删除单条
  const deleteMatch = pathname.match(/^\/api\/admin\/inventory\/([^/]+)$/);
  if (method === 'DELETE' && deleteMatch) {
    const id = deleteMatch[1];
    await fetch(`${supabaseUrl}/rest/v1/inventory?id=eq.${encodeURIComponent(id)}`, {
      method: 'DELETE',
      headers
    });
    return new Response(null, { status: 204, headers: cors });
  }

  // ── POST /api/admin/inventory/batch-delete ── 批量删除（清空全表）
  if (method === 'POST' && pathname === '/api/admin/inventory/batch-delete') {
    await fetch(`${supabaseUrl}/rest/v1/inventory?id=neq.00000000-0000-0000-0000-000000000000`, {
      method: 'DELETE',
      headers
    });
    return jsonResponse({ success: true }, 200, cors);
  }

  // ── POST /api/admin/inventory/batch-insert ── 批量插入
  if (method === 'POST' && pathname === '/api/admin/inventory/batch-insert') {
    const body = await request.json();
    const rows = body.rows || [];
    if (!Array.isArray(rows) || rows.length === 0) {
      return jsonResponse({ error: 'rows array is required' }, 400, cors);
    }
    const resp = await fetch(`${supabaseUrl}/rest/v1/inventory`, {
      method: 'POST',
      headers: { ...headers, 'Prefer': 'return=minimal' },
      body: JSON.stringify(rows)
    });
    if (!resp.ok) {
      const errText = await resp.text();
      return jsonResponse({ error: errText }, resp.status, cors);
    }
    return jsonResponse({ success: true, inserted: rows.length }, 200, cors);
  }

  return jsonResponse({ error: 'Not found' }, 404, cors);
}

// ============================================================
// 主入口
// ============================================================

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const pathname = url.pathname;
    const origin = request.headers.get('Origin') || '*';

    // ── Admin API 路由（/api/admin/*）──
    if (pathname.startsWith('/api/admin/')) {
      return handleAdminRequest(request, url, env);
    }

    // ── OPTIONS 预检（通用）──
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders(origin) });
    }

    const supabaseUrl = env.SUPABASE_URL;
    const supabaseKey = env.SUPABASE_SECRET_KEY;
    const larkAppId = env.LARK_APP_ID;
    const larkAppSecret = env.LARK_APP_SECRET;

    // ── GET 健康检查 ──
    if (request.method === 'GET') {
      return new Response(JSON.stringify({
        status: 'AERONEX Lark Bot is running',
        version: VERSION
      }), { headers: { 'Content-Type': 'application/json' } });
    }

    if (request.method !== 'POST') {
      return new Response('Method Not Allowed', { status: 405 });
    }

    let body;
    try {
      body = await request.json();
    } catch (e) {
      return new Response(JSON.stringify({ code: 0 }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // URL verification（Lark 验证 Webhook）
    if (body.type === 'url_verification') {
      return new Response(JSON.stringify({ challenge: body.challenge }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // ✅ 立即返回 200，异步处理业务逻辑（解决 Lark 重试问题）
    const responsePromise = new Response(JSON.stringify({ code: 0 }), {
      headers: { 'Content-Type': 'application/json' }
    });

    // ✅ event_id 去重
    const header = body.header || {};
    const eventId = header.event_id || '';
    if (await isEventProcessed(eventId, supabaseUrl, supabaseKey)) {
      return responsePromise;
    }

    const event = body.event || {};
    const msg = event.message || {};
    const sender = event.sender || {};
    const chatType = msg.chat_type || '';
    const msgType = msg.message_type || '';

    if (!['p2p', 'group'].includes(chatType) || msgType !== 'text') {
      return responsePromise;
    }

    let contentObj = {};
    try { contentObj = JSON.parse(msg.content || '{}'); } catch (e) {}

    const openId = (sender.sender_id || {}).open_id || '';
    const chatId = msg.chat_id || '';
    const isGroup = chatType === 'group';

    let keyword = '';
    if (isGroup) {
      const textRaw = contentObj.text || '';
      if (!textRaw.includes('@_user_')) return responsePromise;
      keyword = textRaw.replace(/@_user_\d+/g, '').trim();
    } else {
      keyword = (contentObj.text || '').trim();
    }

    if (!keyword || !openId) return responsePromise;

    try {
      const token = await getLarkToken(larkAppId, larkAppSecret);
      const reply = await handleMessage(openId, keyword, supabaseUrl, supabaseKey);
      if (reply === '__EXPORT__') {
        // 导出是异步长操作，独立调用，不阻塞其他消息处理
        await handleExport(openId, token, isGroup, chatId, supabaseUrl, supabaseKey);
      } else if (reply) {
        await sendReply(openId, reply, token, isGroup, chatId);
      }
    } catch (e) {
      // 静默处理异常
    }

    return responsePromise;
  }
};
