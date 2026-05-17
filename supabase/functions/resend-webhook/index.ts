// supabase/functions/resend-webhook/index.ts
// Recebe webhooks do Resend (sent, delivered, opened, clicked, bounced, complained)
// e grava em email_events. Valida via svix signature.
//
// CONFIGURAÇÃO:
// 1. Deploy: supabase functions deploy resend-webhook --no-verify-jwt
// 2. Set RESEND_WEBHOOK_SECRET no Supabase secrets:
//    supabase secrets set RESEND_WEBHOOK_SECRET=<svix_signing_secret>
// 3. No dashboard Resend → Webhooks → Add Endpoint:
//    URL: https://<seu-projeto>.supabase.co/functions/v1/resend-webhook
//    Eventos: email.sent, email.delivered, email.opened, email.clicked,
//             email.bounced, email.complained, email.delivery_delayed

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.0";
import { Webhook } from "https://esm.sh/standardwebhooks@1.0.0";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const WEBHOOK_SECRET = Deno.env.get("RESEND_WEBHOOK_SECRET") || "";

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { autoRefreshToken: false, persistSession: false },
});

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, svix-id, svix-timestamp, svix-signature",
};

function extractTag(tags: any, name: string): string | null {
  if (!Array.isArray(tags)) return null;
  const found = tags.find((t: any) => t?.name === name);
  return found?.value ?? null;
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS_HEADERS });
  if (req.method !== "POST") {
    return new Response("method not allowed", { status: 405, headers: CORS_HEADERS });
  }

  const rawBody = await req.text();
  let payload: any;

  // Valida assinatura svix (Resend usa StandardWebhooks)
  if (WEBHOOK_SECRET) {
    try {
      const svixId = req.headers.get("svix-id") || "";
      const svixTimestamp = req.headers.get("svix-timestamp") || "";
      const svixSignature = req.headers.get("svix-signature") || "";
      const wh = new Webhook(WEBHOOK_SECRET);
      payload = wh.verify(rawBody, {
        "svix-id": svixId,
        "svix-timestamp": svixTimestamp,
        "svix-signature": svixSignature,
      });
    } catch (e) {
      console.error("svix verification failed:", e);
      return new Response(JSON.stringify({ error: "invalid signature" }), {
        status: 401,
        headers: { ...CORS_HEADERS, "Content-Type": "application/json" },
      });
    }
  } else {
    // Sem secret configurado: aceita sem validação (modo dev)
    console.warn("RESEND_WEBHOOK_SECRET não configurado — aceitando sem validar");
    try {
      payload = JSON.parse(rawBody);
    } catch {
      return new Response(JSON.stringify({ error: "invalid json" }), {
        status: 400,
        headers: { ...CORS_HEADERS, "Content-Type": "application/json" },
      });
    }
  }

  // Payload Resend tem estrutura:
  // { type: "email.delivered", created_at: "...", data: { email_id, from, to, subject, tags, ... } }
  const eventType = payload?.type;
  const data = payload?.data || {};
  if (!eventType) {
    return new Response(JSON.stringify({ error: "missing event type" }), {
      status: 400,
      headers: { ...CORS_HEADERS, "Content-Type": "application/json" },
    });
  }

  const resendId = data.email_id || data.id || null;
  const tags = data.tags;
  const userId = extractTag(tags, "user_id");
  const editionId = extractTag(tags, "edition_id");

  // Destinatário (pode ser array ou string)
  let toEmail: string | null = null;
  if (Array.isArray(data.to) && data.to.length > 0) toEmail = data.to[0];
  else if (typeof data.to === "string") toEmail = data.to;

  // Click event details
  const click = data.click || {};
  const link = click.link || null;
  const ip = click.ipAddress || data.ipAddress || null;
  const userAgent = click.userAgent || data.userAgent || null;

  // Bounce details
  const bounceType = data.bounce?.type || null;

  // Fallback: se user_id não veio via tag, tenta lookup pelo email
  let resolvedUserId = userId;
  if (!resolvedUserId && toEmail) {
    try {
      const { data: u } = await supabase
        .from("users")
        .select("id")
        .eq("email", toEmail)
        .single();
      if (u?.id) resolvedUserId = u.id;
    } catch {
      // ignora — email pode não estar no banco
    }
  }

  try {
    await supabase.from("email_events").insert({
      resend_id: resendId,
      user_id: resolvedUserId,
      edition_id: editionId,
      email: toEmail,
      event_type: eventType,
      ip,
      user_agent: userAgent,
      link,
      bounce_type: bounceType,
      payload,
    });
  } catch (e) {
    console.error("insert email_events failed:", e);
    return new Response(JSON.stringify({ error: "insert failed" }), {
      status: 500,
      headers: { ...CORS_HEADERS, "Content-Type": "application/json" },
    });
  }

  return new Response(JSON.stringify({ ok: true }), {
    status: 200,
    headers: { ...CORS_HEADERS, "Content-Type": "application/json" },
  });
});
