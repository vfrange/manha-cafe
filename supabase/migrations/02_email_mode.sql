-- Adiciona coluna `email_mode` na tabela users.
-- Modos: 'coado' (default, análise completa) ou 'espresso' (manchete + 1 frase).
-- Rodar UMA vez no Supabase Dashboard → SQL Editor → New query.

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS email_mode TEXT NOT NULL DEFAULT 'coado';
