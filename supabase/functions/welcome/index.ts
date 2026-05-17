// Edge Function: welcome
// Disparada pelo cadastro.html quando user novo se cadastra.
// Valida que existe e ainda não recebeu welcome, dispara o GitHub workflow.
//
// Setup: Deploy → desmarcar "Verify JWT" no Supabase
// Secrets necessários:
//   - GITHUB_PAT_WELCOME (Personal Access Token fine-grained)
//   - GITHUB_REPO (ex: "vfrange/manha-cafe")

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const GITHUB_TOKEN = Deno.env.get("GITHUB_PAT_WELCOME")!;
const GITHUB_REPO = Deno.env.get("GITHUB_REPO") || "vfrange/manha-cafe";

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, apikey",
};

function json(data: any, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...CORS },
  });
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS });
  }
  if (req.method !== "POST") {
    return json({ error: "method_not_allowed" }, 405);
  }

  let body: any;
  try { body = await req.json(); } catch { return json({ error: "invalid_json" }, 400); }

  const { user_id } = body;
  if (!user_id) return json({ error: "missing_user_id" }, 400);

  // Verifica que existe e ainda não recebeu welcome
  const { data: user, error } = await supabase
    .from("users")
    .select("id, email, active, welcome_sent")
    .eq("id", user_id)
    .maybeSingle();

  if (error) return json({ error: "db_error", message: error.message }, 500);
  if (!user) return json({ error: "user_not_found" }, 404);
  if (!user.active) return json({ error: "user_inactive" }, 400);
  if (user.welcome_sent) return json({ status: "already_sent" });

  // Dispara o workflow
  const url = `https://api.github.com/repos/${GITHUB_REPO}/actions/workflows/welcome.yml/dispatches`;
  const resp = await fetch(url, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${GITHUB_TOKEN}`,
      "Accept": "application/vnd.github.v3+json",
      "Content-Type": "application/json",
      "User-Agent": "Recorte-Welcome",
    },
    body: JSON.stringify({ ref: "main", inputs: { user_id } }),
  });

  if (!resp.ok) {
    const txt = await resp.text();
    return json({ error: "github_dispatch_failed", status: resp.status, detail: txt }, 500);
  }

  return json({ status: "dispatched" });
});
