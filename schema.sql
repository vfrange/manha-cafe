-- ============================================
-- Manhã ☕ — Schema Supabase v4
-- Idempotente: roda quantas vezes quiser.
-- ============================================

-- USERS
create table if not exists users (
  id uuid primary key default gen_random_uuid(),
  email text unique not null,
  name text not null,
  send_hour int not null default 7 check (send_hour between 0 and 23),
  active boolean not null default true,
  created_at timestamptz default now(),
  last_sent_at timestamptz
);

alter table users add column if not exists default_country text default 'BR';
alter table users add column if not exists trending_enabled boolean default true;
alter table users add column if not exists trending_scope text default 'br,global';
-- v4: trending_scope agora aceita CSV ("br,global"). Removendo constraint enum antigo.
alter table users drop constraint if exists users_trending_scope_check;
alter table users add column if not exists trending_country text;

-- v4: campos de telefone
alter table users add column if not exists phone text;                  -- E.164: +5511987654321
alter table users add column if not exists phone_display text;          -- "+55 11 987654321"
alter table users add column if not exists phone_country_code text;     -- "BR"

create index if not exists idx_users_hour_active on users(send_hour, active);

-- TOPICS
create table if not exists topics (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references users(id) on delete cascade,
  label text not null,
  query text not null,
  source text not null default 'curated' check (source in ('curated','custom')),
  color text default '#FFD60A',
  category text, -- v3: economia/tecnologia/politica/etc (pra matching de fontes BR)
  created_at timestamptz default now()
);

alter table topics add column if not exists country text;
alter table topics add column if not exists category text;
create index if not exists idx_topics_user on topics(user_id);

-- ============================================
-- V3: FEEDBACK + PERFIL APRENDIDO
-- ============================================

-- Snapshot do que foi enviado no e-mail — referenciado pelo link de feedback
create table if not exists email_items (
  id text primary key,                       -- token curto ~10 chars
  user_id uuid not null references users(id) on delete cascade,
  kind text not null check (kind in ('news','topic')),
  payload jsonb not null,                    -- {title, source, link, topic_id, topic_label}
  created_at timestamptz default now()
);
create index if not exists idx_email_items_user on email_items(user_id, created_at);

-- Eventos de feedback (👍 / 👎)
create table if not exists feedback_events (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references users(id) on delete cascade,
  email_item_id text references email_items(id) on delete set null,
  signal int not null check (signal in (-1, 1)),
  kind text not null check (kind in ('news','topic')),
  item_summary text,                         -- texto curto do que foi votado (pra Claude depois)
  processed boolean default false,           -- worker semanal marca como true após consolidar
  created_at timestamptz default now()
);
create index if not exists idx_feedback_unprocessed on feedback_events(user_id, processed, created_at);

-- Perfil aprendido (texto livre, atualizado pelo Claude semanalmente)
create table if not exists user_profile (
  user_id uuid primary key references users(id) on delete cascade,
  learned_text text default '',
  paused_topics jsonb default '[]'::jsonb,   -- [{topic_id, label, until}]
  updated_at timestamptz default now()
);

-- ============================================
-- RLS
-- ============================================
alter table users enable row level security;
alter table topics enable row level security;
alter table email_items enable row level security;
alter table feedback_events enable row level security;
alter table user_profile enable row level security;

drop policy if exists "anon upsert users" on users;
create policy "anon upsert users" on users for insert with check (true);
drop policy if exists "anon update users by email" on users;
create policy "anon update users by email" on users for update using (true) with check (true);
drop policy if exists "anon read users" on users;
create policy "anon read users" on users for select using (true);

drop policy if exists "anon insert topics" on topics;
create policy "anon insert topics" on topics for insert with check (true);
drop policy if exists "anon delete topics" on topics;
create policy "anon delete topics" on topics for delete using (true);
drop policy if exists "anon read topics" on topics;
create policy "anon read topics" on topics for select using (true);

-- email_items / feedback / profile: somente service_role (sem policies anon)
