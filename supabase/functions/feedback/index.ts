// Recorte \u2702 \u2014 Edge Function de feedback
// Recebe cliques de "+ mais como essa" / "\u2013 menos como essa" / "pausar tema"
// Endpoint: https://<project>.functions.supabase.co/feedback?i=ID&s=\u00B11&t=TOKEN

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
  const body = `<!DOCTYPE html><html lang="pt-BR"><head>
<meta charset="utf-8"><meta http-equiv="Content-Type" content="text/html; charset=utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${title} \u00B7 Recorte \u2702</title>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:wght@700;900&family=DM+Sans:wght@400;600&display=swap" rel="stylesheet">
<style>
body { margin:0; min-height:100vh; background:#FFF8EC; font-family:'DM Sans',system-ui,sans-serif; display:flex; align-items:center; justify-content:center; padding:24px; color:#0A0A0A; }
.card { max-width:480px; background:#FFF; border:2px solid #0A0A0A; box-shadow:8px 8px 0 #0A0A0A; padding:40px; text-align:center; }
.icon { width:80px; height:80px; margin:0 auto 24px; background:${accent}; border:2px solid #0A0A0A; border-radius:50%; display:flex; align-items:center; justify-content:center; font-size:40px; box-shadow:6px 6px 0 #0A0A0A; }
h1 { font-family:'Fraunces',serif; font-weight:900; font-size:36px; letter-spacing:-0.03em; margin:0 0 14px 0; }
p { font-size:16px; color:#4A4A4A; line-height:1.5; margin:0 0 20px 0; }
.foot { font-size:12px; color:#8A8A85; margin-top:24px; }
.foot strong { color:#0A0A0A; }
</style></head>
<body><div class="card">
<div class="icon">\u2615</div>
<h1>${title}</h1>
<p>${message}</p>
<div class="foot"><strong>Recorte \u2702</strong> \u00B7 sua newsletter aprende com voc\u00EA</div>
</div></body></html>`;
  // Codifica explicitamente como bytes UTF-8 + Content-Length pra evitar
  // que o proxy/CDN interprete o charset errado e fique com emojis quebrados.
  const bytes = new TextEncoder().encode(body);
  return new Response(bytes, {
    headers: {
      "content-type": "text/html; charset=utf-8",
      "content-length": String(bytes.length),
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
      return htmlPage("Link inv\u00E1lido", "Esse link parece incompleto. Tente clicar no bot\u00E3o direto do e-mail.", "#FF5A1F");
    }

    // valida HMAC
    const expected = await hmacSign(itemId, signal);
    if (expected !== token) {
      return htmlPage("Link inv\u00E1lido", "Esse link n\u00E3o passou na verifica\u00E7\u00E3o de seguran\u00E7a.", "#FF5A1F");
    }

    // busca o item
    const { data: item } = await supabase.from("email_items").select("*").eq("id", itemId).single();
    if (!item) {
      return htmlPage("Item n\u00E3o encontrado", "Esse feedback se refere a um item antigo que j\u00E1 n\u00E3o est\u00E1 dispon\u00EDvel.", "#FF5A1F");
    }

    const userId = item.user_id;
    const kind = item.kind as "news" | "topic";
    const payload = item.payload as Record<string, unknown>;

    // monta resumo curto pro Claude consolidar depois
    let summary = "";
    if (kind === "news") {
      summary = `${(payload.title as string) || ""} (fonte: ${(payload.source as string) || "?"})`;
    } else {
      summary = `Tema: ${(payload.topic_label as string) || "?"}`;
    }

    // grava evento
    await supabase.from("feedback_events").insert({
      user_id: userId,
      email_item_id: itemId,
      signal,
      kind,
      item_summary: summary.slice(0, 500),
    });

    // se for "pausar tema" \u2192 adiciona em paused_topics por 7 dias
    let paused_msg = "";
    if (kind === "topic" && signal === -1) {
      const topicId = (payload.topic_id as string) || null;
      const topicLabel = (payload.topic_label as string) || "esse tema";
      const until = new Date();
      until.setDate(until.getDate() + 7);

      // pega perfil atual
      const { data: prof } = await supabase.from("user_profile").select("paused_topics").eq("user_id", userId).single();
      const current = (prof?.paused_topics as Array<Record<string, unknown>>) || [];
      // remove o tema se j\u00E1 estava pausado, depois adiciona com novo prazo
      const filtered = current.filter((p) => p.topic_id !== topicId && p.label !== topicLabel);
      filtered.push({ topic_id: topicId, label: topicLabel, until: until.toISOString() });

      await supabase.from("user_profile").upsert({
        user_id: userId,
        paused_topics: filtered,
        updated_at: new Date().toISOString(),
      });
      paused_msg = ` Voc\u00EA n\u00E3o vai ver <strong>"${topicLabel}"</strong> nos pr\u00F3ximos 7 dias.`;
    }

    // mensagem de retorno
    const isPositive = signal === 1;
    const titleText = isPositive ? "Anotado! \u{1F44D}" : "Anotado! \u{1F44E}";
    const accent = isPositive ? "#FFD60A" : "#FF5A1F";
    let msg = "";
    if (kind === "news") {
      msg = isPositive
        ? "Vou priorizar mais not\u00EDcias como essa nas pr\u00F3ximas edi\u00E7\u00F5es."
        : "Vou diminuir not\u00EDcias parecidas. O algoritmo aprende com cada toque seu.";
    } else {
      msg = isPositive
        ? "Tema refor\u00E7ado \u2014 voc\u00EA vai ver mais sobre isso."
        : `Tema pausado.${paused_msg}`;
    }

    return htmlPage(titleText, msg, accent);
  } catch (err) {
    console.error(err);
    return htmlPage("Algo deu errado", "Tente de novo daqui a pouco. Se persistir, \u00E9 bug \u2014 me avise.", "#FF5A1F");
  }
});
