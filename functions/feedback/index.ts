// Manhã ☕ — Edge Function de feedback
// Recebe cliques de "+ mais como essa" / "– menos como essa" / "pausar tema"
// Endpoint: https://<project>.functions.supabase.co/feedback?i=ID&s=±1&t=TOKEN

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
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${title} · Manhã ☕</title>
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
<div class="icon">☕</div>
<h1>${title}</h1>
<p>${message}</p>
<div class="foot"><strong>Manhã ☕</strong> · sua newsletter aprende com você</div>
</div></body></html>`;
  return new Response(body, {
    headers: { "Content-Type": "text/html; charset=UTF-8" },
  });
}

Deno.serve(async (req) => {
  try {
    const url = new URL(req.url);
    const itemId = url.searchParams.get("i") || "";
    const signal = parseInt(url.searchParams.get("s") || "0");
    const token = url.searchParams.get("t") || "";

    if (!itemId || (signal !== 1 && signal !== -1) || !token) {
      return htmlPage("Link inválido", "Esse link parece incompleto. Tente clicar no botão direto do e-mail.", "#FF5A1F");
    }

    // valida HMAC
    const expected = await hmacSign(itemId, signal);
    if (expected !== token) {
      return htmlPage("Link inválido", "Esse link não passou na verificação de segurança.", "#FF5A1F");
    }

    // busca o item
    const { data: item } = await supabase.from("email_items").select("*").eq("id", itemId).single();
    if (!item) {
      return htmlPage("Item não encontrado", "Esse feedback se refere a um item antigo que já não está disponível.", "#FF5A1F");
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

    // se for "pausar tema" → adiciona em paused_topics por 7 dias
    let paused_msg = "";
    if (kind === "topic" && signal === -1) {
      const topicId = (payload.topic_id as string) || null;
      const topicLabel = (payload.topic_label as string) || "esse tema";
      const until = new Date();
      until.setDate(until.getDate() + 7);

      // pega perfil atual
      const { data: prof } = await supabase.from("user_profile").select("paused_topics").eq("user_id", userId).single();
      const current = (prof?.paused_topics as Array<Record<string, unknown>>) || [];
      // remove o tema se já estava pausado, depois adiciona com novo prazo
      const filtered = current.filter((p) => p.topic_id !== topicId && p.label !== topicLabel);
      filtered.push({ topic_id: topicId, label: topicLabel, until: until.toISOString() });

      await supabase.from("user_profile").upsert({
        user_id: userId,
        paused_topics: filtered,
        updated_at: new Date().toISOString(),
      });
      paused_msg = ` Você não vai ver <strong>"${topicLabel}"</strong> nos próximos 7 dias.`;
    }

    // mensagem de retorno
    const isPositive = signal === 1;
    const titleText = isPositive ? "Anotado! 👍" : "Anotado! 👎";
    const accent = isPositive ? "#FFD60A" : "#FF5A1F";
    let msg = "";
    if (kind === "news") {
      msg = isPositive
        ? "Vou priorizar mais notícias como essa nas próximas edições."
        : "Vou diminuir notícias parecidas. O algoritmo aprende com cada toque seu.";
    } else {
      msg = isPositive
        ? "Tema reforçado — você vai ver mais sobre isso."
        : `Tema pausado.${paused_msg}`;
    }

    return htmlPage(titleText, msg, accent);
  } catch (err) {
    console.error(err);
    return htmlPage("Algo deu errado", "Tente de novo daqui a pouco. Se persistir, é bug — me avise.", "#FF5A1F");
  }
});
