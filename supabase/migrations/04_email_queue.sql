-- ========================================================
-- Migration 04: email_queue
-- Arquitetura prepare → dispatch (decoupling de coleta e envio)
-- ========================================================

create table if not exists public.email_queue (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.users(id) on delete cascade,
  kind text not null check (kind in ('daily','weekly')),
  scheduled_for date not null,           -- pra qual dia esse email é (BRT date)
  subject text not null,
  html text not null,                    -- HTML completo do email pronto pra enviar
  status text not null default 'pending' check (status in ('pending','sending','sent','failed','skipped')),
  attempts int not null default 0,
  error text,                            -- mensagem de erro se status='failed'
  resend_id text,                        -- id retornado pelo Resend após envio
  created_at timestamptz not null default now(),
  sent_at timestamptz                    -- timestamp do envio efetivo
);

-- Índice rápido pra dispatcher achar emails pra enviar
create index if not exists idx_email_queue_pending
  on public.email_queue(scheduled_for, status)
  where status = 'pending';

-- Histórico por usuário
create index if not exists idx_email_queue_user_date
  on public.email_queue(user_id, scheduled_for desc);

-- Idempotência: 1 registro por (user, dia, tipo) — evita preparar 2x o mesmo email
create unique index if not exists uq_email_queue_user_date_kind
  on public.email_queue(user_id, scheduled_for, kind);

comment on table public.email_queue is
  'Fila de emails preparados pelo prepare_*.py, consumida pelo dispatch_emails.py';
comment on column public.email_queue.status is
  'pending → aguardando envio | sending → claimed por um worker | sent → enviado | failed → falhou após retries | skipped → não enviar';
