// supabase/functions/click/index.ts
// GET /c/{short_id} → loga clique, valida URL, 302 redirect ou página de erro amigável.
//
// CONFIGURAÇÃO:
// 1. Deploy: supabase functions deploy click --no-verify-jwt
// 2. Roteamento via Cloudflare/proxy: /c/* → este endpoint
//
// IMPORTANTE:
// - HEAD check no target URL ANTES de redirecionar evita link quebrado pro user
// - Cache curto (5 min) na resposta de status pra não estourar
// - Página de erro estática se URL morta
// - escapeHtml em interpolações pra prevenir XSS via topic_label
// - Bloqueia URLs internas/localhost pra prevenir SSRF

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.0";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { autoRefreshToken: false, persistSession: false },
});

const HEAD_TIMEOUT_MS = 2500;

function escapeHtml(s: string): string {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

// Bloqueia URLs internas pra prevenir SSRF (AWS metadata, localhost, etc).
// Retorna true se URL é segura pra fetch externo.
function isSafeUrl(url: string): boolean {
  try {
    const u = new URL(url);
    if (u.protocol !== "http:" && u.protocol !== "https:") return false;
    const host = u.hostname.toLowerCase();
    // Bloqueia IPs privados conhecidos e hostnames internos
    if (host === "localhost" || host === "0.0.0.0") return false;
    if (host.startsWith("127.")) return false;        // loopback
    if (host.startsWith("10.")) return false;          // RFC 1918
    if (host.startsWith("192.168.")) return false;     // RFC 1918
    if (host.startsWith("169.254.")) return false;     // link-local (AWS metadata)
    if (host.startsWith("172.")) {                     // RFC 1918 (16-31)
      const second = parseInt(host.split(".")[1] || "0", 10);
      if (second >= 16 && second <= 31) return false;
    }
    if (host.endsWith(".internal") || host.endsWith(".local")) return false;
    return true;
  } catch {
    return false;
  }
}

const ERROR_PAGE = (manchete: string) => `<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Notícia indisponível — Recorte ✂</title>
<style>
  :root { --bg:#F2F0E6; --ink:#1A1A1A; --mint:#9FE5C8; --mint-deep:#0A6E50; --soft:#444; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#1a1a1a; --ink:#f0f0f0; --soft:#c0c0c0; }
  }
  body { margin:0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Mulish,sans-serif;
         background:var(--bg); color:var(--ink); line-height:1.6;
         min-height:100vh; display:flex; align-items:center; justify-content:center; padding:24px; }
  .card { max-width:520px; background:rgba(0,0,0,0.02); border-left:6px solid var(--mint-deep);
          padding:36px 32px; }
  .icon { font-size:48px; margin-bottom:16px; }
  h1 { font-family:Fraunces,Georgia,serif; font-size:28px; margin:0 0 12px; }
  p { color:var(--soft); margin:0 0 16px; }
  .manchete { font-style:italic; color:var(--mint-deep); font-weight:600; }
  a { color:var(--mint-deep); font-weight:700; text-decoration:underline; }
</style>
</head>
<body>
<div class="card">
  <div class="icon">📰</div>
  <h1>Essa notícia saiu do ar</h1>
  <p>O link que você clicou foi removido pelo publisher original — pode ter sido movido, despublicado ou colocado atrás de paywall.</p>
  ${manchete ? `<p class="manchete">${escapeHtml(manchete)}</p>` : ""}
  <p>Pode tentar buscar diretamente no Google ou voltar pra <a href="https://recorte.news">recorte.news</a>.</p>
</div>
</body>
</html>`;

async function checkUrlStatus(url: string): Promise<number> {
  // SSRF guardrail: bloqueia URLs internas (mesmo que estejam no banco)
  if (!isSafeUrl(url)) {
    return 0;  // sinal "URL bloqueada por segurança"
  }
  try {
    const controller = new AbortController();
    const tid = setTimeout(() => controller.abort(), HEAD_TIMEOUT_MS);
    const resp = await fetch(url, {
      method: "HEAD",
      redirect: "follow",
      signal: controller.signal,
      headers: { "User-Agent": "Mozilla/5.0 RecorteBot/1.0 (+https://recorte.news)" },
    });
    clearTimeout(tid);
    return resp.status;
  } catch (e) {
    // Timeout/erro: assume OK (deixa o user tentar; alguns sites não respondem HEAD)
    return 200;
  }
}

Deno.serve(async (req) => {
  const url = new URL(req.url);
  // Path pode ser /c/{id} ou /functions/v1/click/{id}
  const parts = url.pathname.split("/").filter(Boolean);
  const shortId = parts[parts.length - 1];

  // Valida formato do short_id (alfanumérico, dash, underscore — 6-12 chars)
  if (!shortId || !/^[A-Za-z0-9_-]{6,12}$/.test(shortId)) {
    return new Response(ERROR_PAGE(""), {
      status: 400,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }

  // Lookup
  const { data: row, error } = await supabase
    .from("link_clicks")
    .select("target_url, last_http_status, last_checked_at, topic_label, click_count")
    .eq("short_id", shortId)
    .single();

  if (error || !row) {
    return new Response(ERROR_PAGE(""), {
      status: 404,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }

  const targetUrl = row.target_url;

  // SSRF guardrail antes de qualquer fetch ou redirect
  if (!isSafeUrl(targetUrl)) {
    return new Response(ERROR_PAGE(row.topic_label || ""), {
      status: 400,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }

  // Health check cache: se checou há menos de 5 min, reusa
  const lastCheckedAt = row.last_checked_at ? new Date(row.last_checked_at).getTime() : 0;
  const fiveMinAgo = Date.now() - 5 * 60 * 1000;
  let status: number;
  if (lastCheckedAt > fiveMinAgo && row.last_http_status) {
    status = row.last_http_status;
  } else {
    status = await checkUrlStatus(targetUrl);
  }

  // Increment counter + log (não bloqueia o redirect)
  const updatePayload: any = {
    click_count: (row.click_count || 0) + 1,
    last_clicked_at: new Date().toISOString(),
    last_http_status: status,
    last_checked_at: new Date().toISOString(),
  };
  // Fire-and-forget
  supabase.from("link_clicks").update(updatePayload).eq("short_id", shortId).then(() => {});

  // 4xx/5xx → página de erro amigável (405 = HEAD não suportado, deixa o user tentar)
  if (status >= 400 && status !== 405) {
    return new Response(ERROR_PAGE(row.topic_label || ""), {
      status: 200,  // 200 pra renderizar bem em todos os clients
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }

  // Redirect 302
  return new Response(null, {
    status: 302,
    headers: {
      "Location": targetUrl,
      "Cache-Control": "no-store",
    },
  });
});
