// Edge Function: manage
// Recebe ações da página /manage com token HMAC pra validação.
// Roda com service_role (acesso total ao banco), mas só executa se HMAC válido.
//
// Setup: Deploy → desmarcar "Verify JWT" no Supabase
//
// Endpoints:
//   POST { action: "get", u, exp, t }                                       → { user, topics }
//   POST { action: "update", u, exp, t, user_updates?, topics_replacement? } → { ok: true }
//   POST { action: "delete", u, exp, t }                                    → { ok: true }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const SECRET = Deno.env.get("FEEDBACK_SECRET")!;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, apikey",
};

function json(data: any, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...CORS_HEADERS },
  });
}

async function hmacVerifyManage(userId: string, exp: string | number, signature: string): Promise<boolean> {
  try {
    const enc = new TextEncoder();
    const key = await crypto.subtle.importKey(
      "raw", enc.encode(SECRET),
      { name: "HMAC", hash: "SHA-256" },
      false, ["sign"]
    );
    const payload = `manage|${userId}|${exp}`;
    const sigBuf = await crypto.subtle.sign("HMAC", key, enc.encode(payload));
    const sigArr = Array.from(new Uint8Array(sigBuf));
    const sigHex = sigArr.map(b => b.toString(16).padStart(2, "0")).join("");
    return sigHex.slice(0, 24) === signature;
  } catch {
    return false;
  }
}

const ALLOWED_USER_FIELDS = new Set([
  "name", "active", "trending_enabled", "trending_scope", "email_mode", "timezone"
]);
const ALLOWED_TOPIC_FIELDS = new Set(["label", "query", "country", "source", "category", "color"]);

function sanitizeUserUpdates(updates: any): Record<string, any> {
  const out: Record<string, any> = {};
  if (!updates || typeof updates !== "object") return out;
  for (const [k, v] of Object.entries(updates)) {
    if (ALLOWED_USER_FIELDS.has(k)) out[k] = v;
  }
  return out;
}

// Sanitiza filtered_items: array de strings, max 20, cada uma trim+sem vazias, max 100 chars
function sanitizeFilteredItems(items: any): string[] {
  if (!Array.isArray(items)) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const item of items) {
    if (typeof item !== "string") continue;
    const trimmed = item.trim().slice(0, 100);
    if (!trimmed) continue;
    const key = trimmed.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(trimmed);
    if (out.length >= 20) break;
  }
  return out;
}

function sanitizeTopics(topics: any, userId: string): any[] {
  if (!Array.isArray(topics)) return [];
  const out: any[] = [];
  for (const t of topics) {
    if (!t || typeof t !== "object") continue;
    const rec: Record<string, any> = { user_id: userId };
    for (const [k, v] of Object.entries(t)) {
      if (ALLOWED_TOPIC_FIELDS.has(k)) rec[k] = v;
    }
    if (rec.label && rec.query && rec.country) {
      rec.source = rec.source || "custom";
      out.push(rec);
    }
  }
  return out;
}

Deno.serve(async (req) => {
  // CORS preflight
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS_HEADERS });
  }

  if (req.method !== "POST") {
    return json({ error: "method_not_allowed" }, 405);
  }

  let body: any;
  try {
    body = await req.json();
  } catch {
    return json({ error: "invalid_json" }, 400);
  }

  const { action, u, exp, t } = body;
  if (!action || !u || !exp || !t) {
    return json({ error: "missing_fields" }, 400);
  }

  // Valida expiração
  const expNum = parseInt(String(exp), 10);
  if (!expNum || Date.now() / 1000 > expNum) {
    return json({ error: "expired" }, 401);
  }

  // Valida HMAC
  const valid = await hmacVerifyManage(u, exp, t);
  if (!valid) {
    return json({ error: "invalid_token" }, 401);
  }

  // Roteia ação
  try {
    if (action === "get") {
      const { data: user, error: uErr } = await supabase
        .from("users")
        .select("id, name, email, active, trending_enabled, trending_scope, send_hour, email_mode, timezone")
        .eq("id", u)
        .maybeSingle();
      if (uErr) throw uErr;
      if (!user) return json({ error: "not_found" }, 404);

      const { data: topics, error: tErr } = await supabase
        .from("topics")
        .select("id, label, query, source, country")
        .eq("user_id", u);
      if (tErr) throw tErr;

      // Carrega filtered_items do user_profile (singular)
      const { data: prof } = await supabase
        .from("user_profile")
        .select("filtered_items")
        .eq("user_id", u)
        .maybeSingle();
      const filtered_items = (prof?.filtered_items as string[]) || [];

      return json({ user, topics: topics || [], filtered_items });
    }

    if (action === "update") {
      // Atualiza campos permitidos do user
      const userUpdates = sanitizeUserUpdates(body.user_updates);
      if (Object.keys(userUpdates).length > 0) {
        const { error } = await supabase.from("users").update(userUpdates).eq("id", u);
        if (error) throw error;
      }

      // Substitui topics: delete all + insert
      if (Array.isArray(body.topics_replacement)) {
        const newTopics = sanitizeTopics(body.topics_replacement, u);
        const { error: delErr } = await supabase.from("topics").delete().eq("user_id", u);
        if (delErr) throw delErr;
        if (newTopics.length > 0) {
          const { error: insErr } = await supabase.from("topics").insert(newTopics);
          if (insErr) throw insErr;
        }
      }

      // Atualiza filtered_items em user_profile (upsert por user_id)
      if (Array.isArray(body.filtered_items)) {
        const cleaned = sanitizeFilteredItems(body.filtered_items);
        const { error: profErr } = await supabase
          .from("user_profile")
          .upsert({ user_id: u, filtered_items: cleaned }, { onConflict: "user_id" });
        if (profErr) throw profErr;
      }
      return json({ ok: true });
    }

    if (action === "delete") {
      const { error } = await supabase.from("users").delete().eq("id", u);
      if (error) throw error;
      return json({ ok: true });
    }

    return json({ error: "unknown_action" }, 400);
  } catch (e) {
    return json({ error: "server_error", message: (e as Error).message }, 500);
  }
});
