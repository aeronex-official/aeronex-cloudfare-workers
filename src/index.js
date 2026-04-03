/**
 * AERONEX Lark Bot - Cloudflare Worker
 * Version: 2.0.0
 * 功能：接收 Lark 消息，查询 Supabase 库存数据
 */

const LARK_BASE_URL = 'https://open.larksuite.com';

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
    // 查询是否存在
    const resp = await fetch(
      `${supabaseUrl}/rest/v1/processed_events?event_id=eq.${eventId}&select=id`,
      { headers: getSupabaseHeaders(supabaseKey) }
    );
    const data = await resp.json();
    if (data && data.length > 0) return true;

    // 不存在则写入
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

    // 检查是否过期（5分钟）
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
    await fetch(`${supabaseUrl}/rest/v1/user_sessions`, {
      method: 'POST',
      headers: {
        ...getSupabaseHeaders(supabaseKey),
        'Prefer': 'resolution=merge-duplicates'
      },
      body: JSON.stringify({
        open_id: openId,
        products: products,
        updated_at: new Date().toISOString()
      })
    });
  } catch (e) {
    // session 失败不影响主流程
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

  // 排序：精确匹配 > 开头匹配 > 包含匹配
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
      products[ean] = {
        ean,
        model: row.model || '',
        dubai_qty: null,
        saudi_qty: null
      };
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

  // 判断是否为选择编号
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

  // 判断是否为 EAN 码
  if (isEan(keyword)) {
    const products = await searchByEan(keyword, supabaseUrl, supabaseKey);
    if (!products.length) {
      return `❌ 未找到 EAN「${keyword}」\n\n请确认EAN码是否正确，或尝试输入型号关键词`;
    }
    await setSession(openId, products, supabaseUrl, supabaseKey);
    return formatProductDetail(products[0]);
  }

  // 型号关键词搜索
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
      body: JSON.stringify({
        receive_id: chatId,
        msg_type: 'text',
        content: JSON.stringify({ text })
      })
    });
  } else {
    await fetch(`${url}?receive_id_type=open_id`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        receive_id: openId,
        msg_type: 'text',
        content: JSON.stringify({ text })
      })
    });
  }
}

// ============================================================
// 主入口
// ============================================================

export default {
  async fetch(request, env) {
    const supabaseUrl = env.SUPABASE_URL;
    const supabaseKey = env.SUPABASE_SECRET_KEY;
    const larkAppId = env.LARK_APP_ID;
    const larkAppSecret = env.LARK_APP_SECRET;

    // GET 健康检查
    if (request.method === 'GET') {
      return new Response(JSON.stringify({
        status: 'AERONEX Lark Bot is running',
        version: '2.0.0'
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

    // URL verification
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

    // 只处理文本消息
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

    // 异步处理（不阻塞响应）
    const ctx = { waitUntil: (p) => p };
    try {
      const token = await getLarkToken(larkAppId, larkAppSecret);
      const reply = await handleMessage(openId, keyword, supabaseUrl, supabaseKey);
      if (reply) {
        await sendReply(openId, reply, token, isGroup, chatId);
      }
    } catch (e) {
      // 静默处理异常，不发送错误消息
    }

    return responsePromise;
  }
};
