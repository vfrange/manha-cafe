-- 09_welcome_sent_at.sql
-- Adiciona coluna welcome_sent_at em users pra evitar enviar weekly no mesmo dia do welcome.
--
-- Lógica de uso:
-- - prepare_weekly pula users com welcome_sent_at::date == hoje
-- - daily_digest.process_user e dispatch_emails.py gravam welcome_sent_at quando welcome_sent passa de false → true
-- - Users antigos (welcome_sent já true há tempos) ficam com welcome_sent_at NULL.
--   NULL é tratado como "OK, manda weekly" (simples e seguro).

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS welcome_sent_at TIMESTAMPTZ;

-- Index pra queries de filtro temporal serem rápidas
CREATE INDEX IF NOT EXISTS users_welcome_sent_at_idx
  ON users (welcome_sent_at)
  WHERE welcome_sent_at IS NOT NULL;

COMMENT ON COLUMN users.welcome_sent_at IS
  'Timestamp do welcome email. Usado pra pular o weekly do mesmo dia do cadastro.';
