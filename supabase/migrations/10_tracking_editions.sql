-- ========================================================
-- Migration 10: tracking + editions (web preview)
-- ========================================================
-- Cria 3 tabelas:
--   editions      → HTML salvo pra web preview público
--   link_clicks   → wrapper de URLs + tracking de cliques
--   email_events  → eventos do Resend (sent, opened, clicked, bounced...)
-- ========================================================


-- ========================================================
-- TABELA: editions
-- ========================================================
-- HTML de cada email enviado, com URL pública /r/{id} pra compartilhar.
-- Edition_id é gerado no Prepare (antes do HTML ser montado).
-- ========================================================
create table if not exists public.editions (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.users(id) on delete cascade,
  queue_id uuid references public.email_queue(id) on delete set null,
  kind text not null check (kind in ('daily','weekly','welcome')),
  subject text not null,
  html text not null,
  scheduled_for date not null,
  sent_at timestamptz,
  view_count int not null default 0,
  last_viewed_at timestamptz,
  resend_id text,
  created_at timestamptz not null default now()
);

-- email_queue ganha referência opcional pra edition (ID gerado no prepare)
alter table public.email_queue
  add column if not exists edition_id uuid references public.editions(id) on delete set null;

create index if not exists idx_email_queue_edition
  on public.email_queue(edition_id)
  where edition_id is not null;

create index if not exists idx_editions_user_date
  on public.editions(user_id, scheduled_for desc);

create index if not exists idx_editions_recent_public
  on public.editions(sent_at desc nulls last)
  where kind != 'welcome' and sent_at is not null;

-- RLS: edition é "público" no sentido de que /r/{id} é acessível por anônimos
-- Mas só quem tem o UUID acessa. Security through obscurity (UUID = 2^122).
alter table public.editions enable row level security;

drop policy if exists "editions_service_role_all" on public.editions;
create policy "editions_service_role_all" on public.editions
  for all using (auth.role() = 'service_role');

-- IMPORTANTE: Anon NÃO tem acesso DIRETO à tabela editions (vaza PII).
-- O acesso público vem via edge function /r/{id} que usa service_role internamente.
-- Anon só pode listar dados não-sensíveis via view editions_public (criada no final).


-- ========================================================
-- TABELA: link_clicks
-- ========================================================
-- Cada link no email vira /c/{short_id} → loga clique + redireciona.
-- ========================================================
create table if not exists public.link_clicks (
  id uuid primary key default gen_random_uuid(),
  short_id text not null unique,  -- nanoid 10 chars, vai no URL /c/{short_id}
  user_id uuid not null references public.users(id) on delete cascade,
  edition_id uuid not null references public.editions(id) on delete cascade,
  target_url text not null,
  source text,                    -- ex: "Google News", "Reddit"
  topic_label text,               -- ex: "Política", "IA"
  position text,                  -- "trending" | "topic_N" | "share"
  click_count int not null default 0,
  last_clicked_at timestamptz,
  last_http_status int,           -- status HTTP da última checagem
  last_checked_at timestamptz,
  created_at timestamptz not null default now()
);

create index if not exists idx_link_clicks_edition
  on public.link_clicks(edition_id);

create index if not exists idx_link_clicks_short_id
  on public.link_clicks(short_id);

-- Agregação rápida pra dashboard
create index if not exists idx_link_clicks_clicked_recent
  on public.link_clicks(last_clicked_at desc)
  where last_clicked_at is not null;

alter table public.link_clicks enable row level security;

drop policy if exists "link_clicks_service_role_all" on public.link_clicks;
create policy "link_clicks_service_role_all" on public.link_clicks
  for all using (auth.role() = 'service_role');


-- ========================================================
-- TABELA: email_events
-- ========================================================
-- Eventos enviados pelo Resend via webhook.
-- event_type: sent, delivered, opened, clicked, bounced, complained, failed, etc.
-- ========================================================
create table if not exists public.email_events (
  id uuid primary key default gen_random_uuid(),
  resend_id text,                 -- ID retornado pelo Resend no envio
  user_id uuid references public.users(id) on delete set null,
  edition_id uuid references public.editions(id) on delete set null,
  email text,                     -- destinatário
  event_type text not null,
  ip text,                        -- IP do user (em opens/clicks)
  user_agent text,                -- UA do user
  link text,                      -- link clicado (só em 'clicked')
  bounce_type text,               -- 'hard' ou 'soft' (em bounces)
  payload jsonb,                  -- payload completo do webhook (debug)
  created_at timestamptz not null default now()
);

create index if not exists idx_email_events_user
  on public.email_events(user_id, created_at desc);

create index if not exists idx_email_events_edition
  on public.email_events(edition_id);

create index if not exists idx_email_events_type
  on public.email_events(event_type, created_at desc);

create index if not exists idx_email_events_resend_id
  on public.email_events(resend_id)
  where resend_id is not null;

alter table public.email_events enable row level security;

drop policy if exists "email_events_service_role_all" on public.email_events;
create policy "email_events_service_role_all" on public.email_events
  for all using (auth.role() = 'service_role');


-- ========================================================
-- VIEWS DE AGREGAÇÃO (pra Dashboard admin) — sem PII, acessíveis por anon
-- ========================================================
-- Stats por dia: total enviados, opens, clicks, bounces
create or replace view public.email_stats_daily as
select
  date(created_at at time zone 'America/Sao_Paulo') as dia,
  count(*) filter (where event_type = 'email.sent') as enviados,
  count(*) filter (where event_type = 'email.delivered') as entregues,
  count(*) filter (where event_type = 'email.opened') as abertos,
  count(distinct resend_id) filter (where event_type = 'email.opened') as opens_unicos,
  count(*) filter (where event_type = 'email.clicked') as cliques,
  count(*) filter (where event_type in ('email.bounced','email.complained')) as bounces_complaints
from public.email_events
group by 1
order by 1 desc;

-- Eventos recentes sem PII (sem email, ip, user_agent, payload) — pra dashboard
create or replace view public.email_events_public as
select
  id,
  event_type,
  edition_id,
  -- truncamos o link pra evitar leak de query params em URLs sensíveis
  substring(link from 1 for 80) as link,
  bounce_type,
  created_at
from public.email_events
order by created_at desc;

-- Cliques agregados por URL — pra dashboard (sem PII; target_url já é público)
create or replace view public.link_clicks_summary as
select
  short_id,
  target_url,
  source,
  topic_label,
  click_count,
  last_clicked_at,
  last_http_status
from public.link_clicks
where click_count > 0
order by click_count desc;

-- Edições públicas — só dados não-sensíveis pra arquivo.html e admin
-- (subject pode ter nome do user na saudação, mas é aceitável; html NÃO exposto)
create or replace view public.editions_public as
select
  id,
  kind,
  scheduled_for,
  sent_at,
  view_count,
  -- subject pode conter "Wesley" mas é aceitável pra mostrar no arquivo
  subject
from public.editions
where sent_at is not null
  and kind != 'welcome';

-- Stats globais (count de users ativos, edições, etc) — não expõe individuais
create or replace view public.recorte_stats as
select
  (select count(*) from public.users where active = true) as users_ativos,
  (select count(*) from public.users) as users_total,
  (select count(*) from public.editions where sent_at is not null) as edicoes_enviadas,
  (select count(*) from public.link_clicks where click_count > 0) as links_clicados,
  (select count(*) from public.email_events where event_type = 'email.opened') as opens_total,
  (select count(*) from public.email_events where event_type = 'email.clicked') as cliques_total,
  (select count(*) from public.email_events where event_type = 'email.bounced') as bounces_total,
  (select count(*) from public.email_events where event_type = 'email.complained') as complaints_total,
  (select count(*) from public.email_events where event_type = 'email.sent') as sent_total;

-- ATENÇÃO: views são security_invoker por default em Postgres 15+,
-- então elas respeitam RLS da tabela base. Pra deixar acessível ao anon,
-- temos que torná-las security_definer OU criar policies na tabela.
-- Usamos security_definer pra não relaxar RLS das tabelas base.

alter view public.email_stats_daily set (security_invoker = false);
alter view public.email_events_public set (security_invoker = false);
alter view public.link_clicks_summary set (security_invoker = false);
alter view public.editions_public set (security_invoker = false);
alter view public.recorte_stats set (security_invoker = false);

-- Garante leitura pública dessas views agregadas
grant select on public.email_stats_daily to anon;
grant select on public.email_events_public to anon;
grant select on public.link_clicks_summary to anon;
grant select on public.editions_public to anon;
grant select on public.recorte_stats to anon;


-- ========================================================
-- COMMENTS pra documentar
-- ========================================================
comment on table public.editions is
  'HTML completo de cada edição enviada. Servido publicamente via /r/{id} pra compartilhamento.';

comment on table public.link_clicks is
  'Wrapper de URLs externas. Cada link no email vira /c/{short_id} que loga clique e redireciona.';

comment on table public.email_events is
  'Eventos do Resend webhook: sent, opened, clicked, bounced, complained, delivered.';
