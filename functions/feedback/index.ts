// Recorte X - Edge Function de feedback
// Recebe cliques de "+ mais como essa" / "- menos como essa" / "pausar tema"
// Endpoint: https://<project>.functions.supabase.co/feedback?i=ID&s=+/-1&t=TOKEN

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { encodeHex } from "https://deno.land/std@0.224.0/encoding/hex.ts";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const FEEDBACK_SECRET = Deno.env.get("FEEDBACK_SECRET")!;

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

function htmlPage(title: string, message: string, accent = "#FFD60A"): Response {
  // Emojis e acentos usando escape sequences Unicode pra garantir
  // que o source eh ASCII puro (zero risco de double-encoding no editor/clipboard).
  // JavaScript expande \u#### em runtime pro caractere correto, e o Response
  // serve bytes UTF-8 corretos sem nenhuma re-codificacao intermediaria.
  const body = `<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${title} \u00b7 Recorte \u2702</title>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:wght@700;900&family=DM+Sans:wght@400;600&display=swap" rel="stylesheet">
<style>
body { margin:0; min-height:100vh; background:#FFF8EC; font-family:'DM Sans',system-ui,sans-serif; display:flex; align-items:center; justify-content:center; padding:24px; color:#0A0A0A; }
.card { max-width:480px; background:#FFF; border:2px solid #0A0A0A; box-shadow:8px 8px 0 #0A0A0A; padding:40px; text-align:center; }
.icon { width:80px; height:80px; margin:0 auto 24px; background:${accent}; border:2px solid #0A0A0A; border-radius:50%; display:flex; align-items:center; justify-content:center; font-size:40px; box-shadow:6px 6px 0 #0A0A0A; }
h1 { font-family:'Fraunces',serif; font-weight:900; font-size:36px; letter-spacing:-0.03em; margin:0 0 14px 0; }
p { font-size:16px; color:#4A4A4A; line-height:1.5; margin:0 0 20px 0; }
.foot { font-size:12px; color:#8A8A85; margin-top:24px; }
.foot strong { color:#0A0A0A; }
</style>
</head>
<body>
<div class="card">
<div class="icon">\u2615</div>
<h1>${title}</h1>
<p>${message}</p>
<div class="foot"><strong>Recorte \u2702</strong> \u00b7 sua newsletter aprende com voc\u00ea</div>
</div>
</body>
</html>`;

  return new Response(body, {
    headers: {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

Deno.serve(async (req) => {
  try {
    const url = new URL(req.url);
    const itemId = url.searchParams.get("i") || "";
    const signal = parseInt(url.searchParams.get("s") || "0");
    const token = url.searchParams.get("t") || "";

    if (!itemId || (signal !== 1 && signal !== -1) || !token) {
      return htmlPage("Link inv\u00e1lido", "Esse link parece incompleto. Tente clicar no bot\u00e3o direto do e-mail.", "#FF5A1F");
    }

    const expected = await hmacSign(itemId, signal);
    if (expected !== token) {
      return htmlPage("Link inv\u00e1lido", "Esse link n\u00e3o passou na verifica\u00e7\u00e3o de seguran\u00e7a.", "#FF5A1F");
    }

    const { data: item } = await supabase.from("email_items").select("*").eq("id", itemId).single();
    if (!item) {
      return htmlPage("Item n\u00e3o encontrado", "Esse feedback se refere a um item antigo que j\u00e1 n\u00e3o est\u00e1 dispon\u00edvel.", "#FF5A1F");
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

      return htmlPage(
        "Anotado! \u{1F44E}",
        `Tema pausado. Voc\u00ea n\u00e3o vai ver <strong>"${topicLabel}"</strong> nos pr\u00f3ximos 7 dias.`,
      );
    }

    if (kind === "news") {
      await supabase.from("feedback_events").insert({
        user_id: userId,
        item_id: itemId,
        kind: "news_reaction",
        signal: signal,
        payload: payload,
      });

      if (signal === 1) {
        return htmlPage(
          "Anotado! \u{1F44D}",
          `\u00d3timo! Vou trazer mais coisas como <strong>"${payload.title || 'essa'}"</strong>.`,
        );
      } else {
        return htmlPage(
          "Anotado! \u{1F44E}",
          `Vou trazer menos coisas como <strong>"${payload.title || 'essa'}"</strong>.`,
          "#FF5A1F",
        );
      }
    }

    return htmlPage("A\u00e7\u00e3o n\u00e3o reconhecida", "Esse tipo de feedback n\u00e3o foi processado.", "#FF5A1F");
  } catch (e) {
    console.error("feedback error:", e);
    return htmlPage("Algo deu errado", "N\u00e3o conseguimos registrar seu feedback. Tente de novo daqui a pouco.", "#FF5A1F");
  }
});
