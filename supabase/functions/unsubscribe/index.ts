// Recorte - Edge Function unsubscribe
// Suporta GET (link clicado pelo usuario no rodape do email)
// E POST (RFC 8058 List-Unsubscribe-Post=One-Click — Gmail/Apple Mail disparam automaticamente)
//
// URL: https://{project}.supabase.co/functions/v1/unsubscribe?u={user_id}&t={token}
// Token HMAC-SHA256 do payload "unsub|{user_id}" truncado em 24 chars (mesmo SECRET do feedback)

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { encodeHex } from "https://deno.land/std@0.224.0/encoding/hex.ts";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const FEEDBACK_SECRET = Deno.env.get("FEEDBACK_SECRET")!;

const CONFIRM_PAGE = "https://recorte.news/unsubscribed.html";

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

async function hmacSign(userId: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(FEEDBACK_SECRET),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign(
    "HMAC",
    key,
    new TextEncoder().encode(`unsub|${userId}`),
  );
  return encodeHex(new Uint8Array(sig)).slice(0, 24);
}

async function processUnsub(userId: string, token: string): Promise<{ok: boolean, error?: string}> {
  if (!userId || !token) return { ok: false, error: "missing params" };

  const expected = await hmacSign(userId);
  if (expected !== token) return { ok: false, error: "invalid token" };

  // Marca o user como inativo
  const { error } = await supabase
    .from("users")
    .update({ active: false })
    .eq("id", userId);

  if (error) {
    console.error("unsub update failed:", error);
    return { ok: false, error: "db error" };
  }

  return { ok: true };
}

Deno.serve(async (req) => {
  const url = new URL(req.url);
  const userId = url.searchParams.get("u") || "";
  const token = url.searchParams.get("t") || "";

  // === POST (one-click RFC 8058) ===
  // Gmail/Apple Mail disparam isso automaticamente sem interação do user
  if (req.method === "POST") {
    const result = await processUnsub(userId, token);
    if (result.ok) {
      return new Response("Unsubscribed", { status: 200 });
    }
    return new Response(`Failed: ${result.error}`, { status: 400 });
  }

  // === GET (clique no link do footer) ===
  if (req.method === "GET") {
    const result = await processUnsub(userId, token);
    if (result.ok) {
      // Redirect pra pagina de confirmacao
      return new Response(null, {
        status: 302,
        headers: { Location: `${CONFIRM_PAGE}?ok=1` },
      });
    }
    // Erro: redirect com flag
    return new Response(null, {
      status: 302,
      headers: { Location: `${CONFIRM_PAGE}?err=${encodeURIComponent(result.error || "unknown")}` },
    });
  }

  return new Response("Method not allowed", { status: 405 });
});
