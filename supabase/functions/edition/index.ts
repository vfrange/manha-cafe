// supabase/functions/edition/index.ts
// GET /r/{edition_id} → serve HTML público da edição com OG meta tags injetadas.
//
// CONFIGURAÇÃO:
// 1. Deploy: supabase functions deploy edition --no-verify-jwt
// 2. Roteamento via Vercel/Netlify proxy: /r/* → este endpoint
//
// Features:
// - OG meta tags injetadas (og:title, og:image, og:description) pra share bonito
// - view_count incrementa a cada visita
// - Cache 1h pra reduzir carga

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.0";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
// PNG estática pra og:image. Se não setada, OG image é omitida (Twitter/WhatsApp
// mostram preview text-only). Pra ativar, suba uma PNG 1200x630 em
// recorte.news/og-default.png e set OG_IMAGE_URL no Supabase secrets.
const OG_IMAGE_URL = Deno.env.get("OG_IMAGE_URL") || "";

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { autoRefreshToken: false, persistSession: false },
});

function isUuid(s: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s);
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function injectOgTags(html: string, opts: {
  title: string; description: string; imageUrl: string; canonicalUrl: string;
}): string {
  const { title, description, imageUrl, canonicalUrl } = opts;
  const og = `
    <meta property="og:type" content="article">
    <meta property="og:title" content="${escapeHtml(title)}">
    <meta property="og:description" content="${escapeHtml(description)}">
    <meta property="og:image" content="${escapeHtml(imageUrl)}">
    <meta property="og:url" content="${escapeHtml(canonicalUrl)}">
    <meta property="og:site_name" content="Recorte ✂">
    <meta name="twitter:card" content="summary_large_image">
    <meta name="twitter:title" content="${escapeHtml(title)}">
    <meta name="twitter:description" content="${escapeHtml(description)}">
    <meta name="twitter:image" content="${escapeHtml(imageUrl)}">
    <link rel="canonical" href="${escapeHtml(canonicalUrl)}">
  `;
  // Substitui no <head> — preserva o resto do email html
  if (html.includes("</head>")) {
    return html.replace("</head>", `${og}\n</head>`);
  }
  // Fallback: prepende no início
  return og + html;
}

const NOT_FOUND_PAGE = `<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Edição não encontrada — Recorte ✂</title>
<style>
  body { margin:0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
         background:#F2F0E6; color:#1A1A1A; min-height:100vh;
         display:flex; align-items:center; justify-content:center; padding:24px; }
  .card { max-width:520px; text-align:center; padding:32px; }
  h1 { font-family:Fraunces,Georgia,serif; font-size:32px; }
  a { color:#0A6E50; font-weight:700; }
</style>
</head><body>
<div class="card">
  <div style="font-size:48px;margin-bottom:16px;">✂</div>
  <h1>Edição não encontrada</h1>
  <p>Esse link pode ter expirado ou estar incorreto.</p>
  <p><a href="https://recorte.news">Visite recorte.news</a></p>
</div>
</body></html>`;

Deno.serve(async (req) => {
  const url = new URL(req.url);
  const parts = url.pathname.split("/").filter(Boolean);
  const editionId = parts[parts.length - 1];

  if (!editionId || !isUuid(editionId)) {
    return new Response(NOT_FOUND_PAGE, {
      status: 404,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }

  const { data: edition, error } = await supabase
    .from("editions")
    .select("id, subject, html, sent_at, kind, view_count")
    .eq("id", editionId)
    .single();

  if (error || !edition) {
    return new Response(NOT_FOUND_PAGE, {
      status: 404,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }

  // Increment view_count (fire-and-forget)
  supabase
    .from("editions")
    .update({
      view_count: (edition.view_count || 0) + 1,
      last_viewed_at: new Date().toISOString(),
    })
    .eq("id", editionId)
    .then(() => {});

  // Inject OG tags
  const canonicalUrl = `https://recorte.news/r/${editionId}`;
  const description = edition.kind === "weekly"
    ? "Sua semana em notícias, recortada pra você."
    : edition.kind === "welcome"
    ? "Sua primeira edição do Recorte ✂."
    : "Suas notícias do dia, recortadas pra você.";

  const html = injectOgTags(edition.html, {
    title: edition.subject || "Recorte ✂",
    description,
    imageUrl: DEFAULT_OG_IMAGE,
    canonicalUrl,
  });

  return new Response(html, {
    status: 200,
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "public, max-age=3600",
      "X-Robots-Tag": "noindex",  // edições têm dados pessoais → não indexar
    },
  });
});
