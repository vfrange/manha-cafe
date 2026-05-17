-- Adiciona timestamp do último envio do Recorte da Semana.
-- Rodar UMA vez no Supabase Dashboard → SQL Editor → New query.

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS last_weekly_sent_at TIMESTAMPTZ;
