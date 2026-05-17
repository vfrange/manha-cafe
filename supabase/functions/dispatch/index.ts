// Recorte ✂ — Edge Function dispatch
// Backup do dispatch_emails.py: pode ser chamado via HTTP por cron externo (cron-job.org)
// Garante envio às 6h em ponto, mesmo se o GitHub Actions falhar.
//
// Idempotente: usa claim atomic via status 'sending' (mesmo padrão do Python).
// Se o GitHub já disparou tudo, essa função vai ver queue vazia e retornar OK.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const RESEND_API_KEY = Deno.env.get("RESEND_API_KEY")!;
const FROM_EMAIL = Deno.env.get("RESEND_FROM") || "Recorte News <hoje@recorte.news>";
const DISPATCH_SECRET = Deno.env.get("DISPATCH_SECRET")!;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

const MAX_ATTEMPTS = 5;
const CONCURRENCY = 8;

async function sendOne(row: any): Promise<{status: string, info: string}> {
  const qid = row.id;
  const userId = row.user_id;
  const attempts = row.attempts || 0;

  // 1. Claim atomic
  const { data: claimed } = await supabase
    .from("email_queue")
    .update({ status: "sending", attempts: attempts + 1 })
    .eq("id", qid)
    .eq("status", "pending")
    .select();

  if (!claimed || claimed.length === 0) {
    return { status: "skipped", info: "outro worker pegou" };
  }

  try {
    // 2. Busca user
    const { data: user } = await supabase
      .from("users").select("email,name,active").eq("id", userId).single();

    if (!user) {
      await supabase.from("email_queue").update({
        status: "failed", error: "user não encontrado",
      }).eq("id", qid);
      return { status: "failed", info: "user não encontrado" };
    }

    if (!user.active) {
      await supabase.from("email_queue").update({
        status: "skipped", error: "user inativo",
      }).eq("id", qid);
      return { status: "skipped", info: "user inativo" };
    }

    // 3. Envia via Resend
    const resp = await fetch("https://api.resend.com/emails", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${RESEND_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        from: FROM_EMAIL,
        to: user.email,
        subject: row.subject,
        html: row.html,
      }),
    });

    if (!resp.ok) {
      const errText = await resp.text();
      throw new Error(`Resend ${resp.status}: ${errText.slice(0, 200)}`);
    }

    const result = await resp.json();
    const resendId = result?.id || null;

    // 4. Marca sent
    await supabase.from("email_queue").update({
      status: "sent",
      sent_at: new Date().toISOString(),
      resend_id: resendId,
    }).eq("id", qid);

    // 5. Atualiza last_sent_at do user
    await supabase.from("users").update({
      last_sent_at: new Date().toISOString(),
      welcome_sent: true,
    }).eq("id", userId);

    return { status: "sent", info: user.email };
  } catch (err: any) {
    const newAttempts = attempts + 1;
    const newStatus = newAttempts >= MAX_ATTEMPTS ? "failed" : "pending";
    await supabase.from("email_queue").update({
      status: newStatus,
      error: String(err).slice(0, 500),
    }).eq("id", qid);
    return { status: "error", info: String(err).slice(0, 200) };
  }
}

// Roda promessas em lotes de CONCURRENCY
async function runInBatches<T, R>(items: T[], fn: (it: T) => Promise<R>, size: number): Promise<R[]> {
  const results: R[] = [];
  for (let i = 0; i < items.length; i += size) {
    const batch = items.slice(i, i + size);
    const r = await Promise.all(batch.map(fn));
    results.push(...r);
  }
  return results;
}

Deno.serve(async (req) => {
  try {
    // 1. Auth: aceita Bearer token OU query ?secret=
    const auth = req.headers.get("Authorization") || "";
    const url = new URL(req.url);
    const querySecret = url.searchParams.get("secret") || "";
    const bearerToken = auth.replace(/^Bearer\s+/i, "");
    const providedSecret = bearerToken || querySecret;

    if (!DISPATCH_SECRET || providedSecret !== DISPATCH_SECRET) {
      return new Response("Unauthorized", { status: 401 });
    }

    // 2. Parâmetros
    const kind = url.searchParams.get("kind") || "any"; // daily | weekly | any
    const dateParam = url.searchParams.get("date"); // YYYY-MM-DD
    const today = dateParam || new Date().toLocaleDateString("sv-SE", { timeZone: "America/Sao_Paulo" });

    // 3. Busca pending
    let query = supabase.from("email_queue").select("*")
      .eq("status", "pending").eq("scheduled_for", today);
    if (kind !== "any") query = query.eq("kind", kind);
    const { data: pending, error } = await query;

    if (error) throw error;
    const items = pending || [];

    if (items.length === 0) {
      return new Response(JSON.stringify({
        ok: true, sent: 0, info: "queue vazia",
        kind, date: today,
      }), { headers: { "content-type": "application/json" } });
    }

    // 4. Dispara em paralelo
    const results = await runInBatches(items, sendOne, CONCURRENCY);

    const counts = { sent: 0, failed: 0, skipped: 0, error: 0 };
    for (const r of results) {
      counts[r.status as keyof typeof counts] = (counts[r.status as keyof typeof counts] || 0) + 1;
    }

    return new Response(JSON.stringify({
      ok: true,
      kind, date: today,
      total: items.length,
      ...counts,
    }), {
      headers: { "content-type": "application/json" },
    });
  } catch (err: any) {
    console.error("dispatch error", err);
    return new Response(JSON.stringify({
      ok: false, error: String(err).slice(0, 500),
    }), {
      status: 500,
      headers: { "content-type": "application/json" },
    });
  }
});
