/**
 * AERONEX Lark Bot + Admin API - Cloudflare Worker
 * Version: 2.1.1
 * 功能：
 *   - 接收 Lark 消息，查询 Supabase 库存数据
 *   - /api/admin/* 提供库存管理 REST API（需 X-Admin-Password 验证）
 * Changelog:
 *   2.1.1 - 修复 setSession() 使用 UPSERT，解决多用户 session 丢失导致"查询已过期"的 Bug
 */

const LARK_BASE_URL = 'https://open.larksuite.com';

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
  try {
    // 使用 Supabase UPSERT（POST + Prefer: resolution=merge-duplicates）
    // open_id 存在 → UPDATE products + updated_at
    // open_id 不存在 → INSERT 新记录
    // 依赖 user_sessions.open_id 的 UNIQUE 约束（已在 Supabase 建立）
    // 修复原因：旧的 PATCH → content-range 判断 → SELECT → INSERT 三步走逻辑
    //   在 Supabase 返回 content-range 为 null 时判断失效，导致多用户 session 无法更新
    await fetch(`${supabaseUrl}/rest/v1/user_sessions`, {
      method: 'POST',
      headers: {
        ...getSupabaseHeaders(supabaseKey),
        'Prefer': 'resolution=merge-duplicates,return=minimal'
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
  const resp = await fetch(
    `${supabaseUrl}/rest/v1/inventory?model=ilike.*${encodeURIComponent(keyword)}*&select=ean,model,warehouse,available_qty&limit=500`,
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
  const products = {};
  for (const row of rows) {
    const ean = String(row.ean || '').trim();
    if (!ean) continue;
    if (!products[ean]) {
      products[ean] = { ean, model: row.model || '', dubai_qty: null, saudi_qty: null };
    }
    const qty = row.available_qty ?? 0;
    if (row.warehouse && row.warehouse.includes('Dubai')) {
      products[ean].dubai_qty = qty;
    } else if (row.warehouse && row.warehouse.includes('Saudi')) {
      products[ean].saudi_qty = qty;
    }
  }
  return Object.values(products);
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
        version: '2.1.1'
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
      if (reply) {
        await sendReply(openId, reply, token, isGroup, chatId);
      }
    } catch (e) {
      // 静默处理异常
    }

    return responsePromise;
  }
};
