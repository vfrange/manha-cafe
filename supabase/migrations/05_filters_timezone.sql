-- ========================================================
-- Migration 05: filtros (lista de temas/veículos que NÃO quero)
-- + timezone do user (capturado no navegador)
-- ========================================================

-- 1) Filtros: armazenados em user_profile (singular)
--    Cada item é um texto livre — pode ser tema ("celebridades", "política de direita")
--    ou veículo ("Carta Capital", "UOL"). Limitado a 20 no client/edge function.
alter table public.user_profile
  add column if not exists filtered_items jsonb not null default '[]'::jsonb;

comment on column public.user_profile.filtered_items is
  'Lista de strings: temas/veículos que o user NÃO quer receber. Max 20 itens.';

-- 2) Timezone do user (capturado via Intl.DateTimeFormat no cadastro)
--    Usado pra saudação adequada no welcome email (Bom dia/tarde/noite no fuso real).
--    Daily/Weekly continuam usando BRT (cron é BRT).
alter table public.users
  add column if not exists timezone text not null default 'America/Sao_Paulo';

comment on column public.users.timezone is
  'IANA timezone do user (ex: America/Sao_Paulo, Europe/Lisbon). Default = BRT. Usado pra saudação no welcome.';

-- RLS já está habilitada (não precisa mexer).
