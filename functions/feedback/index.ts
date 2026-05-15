// Recorte - Edge Function de feedback (redirect mode)
// Registra o evento no banco e redireciona pra pagina estatica de confirmacao
// no recorte.news, evitando o CSP/sandbox do Supabase Edge Runtime.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { encodeHex } from "https://deno.land/std@0.224.0/encoding/hex.ts";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const FEEDBACK_SECRET = Deno.env.get("FEEDBACK_SECRET")!;

// Onde a pagina estatica de confirmacao esta hospedada (sem CSP/sandbox)
const CONFIRM_PAGE = "https://recorte.news/feedback.html";

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

async function hmacSign(itemId: string, signal: number): Promise<string> {
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
    new TextEncoder().encode(`${itemId}:${signal}`),
  );
  return encodeHex(new Uint8Array(sig)).slice(0, 12);
}

function redirect(kind: string, label = "", title = ""): Response {
  const params = new URLSearchParams({ kind });
  if (label) params.set("label", label);
  if (title) params.set("title", title);
  return new Response(null, {
    status: 302,
    headers: { Location: `${CONFIRM_PAGE}?${params.toString()}` },
  });
}

Deno.serve(async (req) => {
  try {
    const url = new URL(req.url);
    const itemId = url.searchParams.get("i") || "";
    const signal = parseInt(url.searchParams.get("s") || "0");
    const token = url.searchParams.get("t") || "";

    if (!itemId || (signal !== 1 && signal !== -1) || !token) {
      return redirect("invalid");
    }

    const expected = await hmacSign(itemId, signal);
    if (expected !== token) {
      return redirect("invalid_token");
    }

    const { data: item } = await supabase.from("email_items").select("*").eq("id", itemId).single();
    if (!item) {
      return redirect("not_found");
    }

    const userId = item.user_id;
    const kind = item.kind;
    const payload = item.payload || {};

    if (kind === "topic" && signal === -1) {
      const topicLabel = payload.topic_label || "esse tema";
      const until = new Date();
      until.setDate(until.getDate() + 7);

      const { data: prof } = await supabase.from("user_profile").select("paused_topics").eq("user_id", userId).single();
      const paused = (prof?.paused_topics as Array<{label: string; until: string}>) || [];
      const filtered = paused.filter((p) => p.label !== topicLabel);
      filtered.push({ label: topicLabel, until: until.toISOString() });

      await supabase.from("user_profile").upsert({
        user_id: userId,
        paused_topics: filtered,
      }, { onConflict: "user_id" });

      await supabase.from("feedback_events").insert({
        user_id: userId,
        item_id: itemId,
        kind: "topic_pause_7d",
        signal: -1,
        payload: { topic_label: topicLabel },
      });

      return redirect("topic_paused", topicLabel);
    }

    if (kind === "news") {
      await supabase.from("feedback_events").insert({
        user_id: userId,
        item_id: itemId,
        kind: "news_reaction",
        signal: signal,
        payload: payload,
      });

      const title = payload.title || "essa";
      return redirect(signal === 1 ? "news_more" : "news_less", "", title);
    }

    return redirect("unknown");
  } catch (e) {
    console.error("feedback error:", e);
    return redirect("error");
  }
});
