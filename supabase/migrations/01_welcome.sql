-- Adiciona coluna `welcome_sent` na tabela users.
-- Rodar UMA vez no Supabase Dashboard → SQL Editor → New query → cola tudo → Run.

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS welcome_sent BOOLEAN NOT NULL DEFAULT false;

-- Marca usuários antigos como já tendo recebido (pra não disparar welcome retroativo)
UPDATE users SET welcome_sent = true WHERE last_sent_at IS NOT NULL;
