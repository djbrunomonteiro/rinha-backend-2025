-- Extensões necessárias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Tabela principal de pagamentos
CREATE TABLE payments_queue (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  correlation_id UUID NOT NULL,
  amount DECIMAL,
  requested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  status VARCHAR(20) NOT NULL DEFAULT 'pending',
  retries INT NOT NULL DEFAULT 0,
  url TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de resumo dos pagamentos aprovados
CREATE TABLE payments_summary (
  url_type VARCHAR(20) PRIMARY KEY,  -- 'default' ou 'fallback'
  total_requests INT NOT NULL DEFAULT 0,
  total_amount DECIMAL NOT NULL DEFAULT 0
);

CREATE OR REPLACE FUNCTION update_payments_summary() RETURNS TRIGGER AS $$
DECLARE
  url_category VARCHAR(20);
BEGIN
  -- Define a categoria com base na url (default ou fallback)
  IF NEW.url IS NULL THEN
    RETURN NEW;
  END IF;

  IF LOWER(NEW.url) LIKE '%default%' THEN
    url_category := 'default';
  ELSIF LOWER(NEW.url) LIKE '%fallback%' THEN
    url_category := 'fallback';
  ELSE
    -- Se não for nenhum dos dois, não atualiza
    RETURN NEW;
  END IF;

  -- Se o status não for 'approved', não atualiza os totais
  IF NEW.status <> 'approved' THEN
    RETURN NEW;
  END IF;

  -- Atualiza o registro existente ou insere novo
  INSERT INTO payments_summary (url_type, total_requests, total_amount)
  VALUES (url_category, 1, NEW.amount)
  ON CONFLICT (url_type) DO UPDATE
    SET total_requests = payments_summary.total_requests + 1,
        total_amount = payments_summary.total_amount + EXCLUDED.total_amount;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_payments_summary
AFTER INSERT OR UPDATE ON payments_queue
FOR EACH ROW
WHEN (NEW.status = 'approved')
EXECUTE FUNCTION update_payments_summary();


-- 1. Criação da tabela
CREATE TABLE health_checks (
  id SERIAL PRIMARY KEY,
  url TEXT NOT NULL,
  is_healthy BOOLEAN NOT NULL,
  min_response_time INTEGER NOT NULL,
  checked_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Índices
CREATE INDEX idx_health_url ON health_checks(url);
CREATE INDEX idx_health_checked_at ON health_checks(checked_at DESC);

-- 3. Adiciona restrição de unicidade por URL
ALTER TABLE health_checks ADD CONSTRAINT unique_url UNIQUE (url);

-- 4. Inserção inicial com prevenção de duplicatas
INSERT INTO health_checks (url, is_healthy, min_response_time)
VALUES
  ('http://payment-processor-default:8080', true, 100),
  ('http://payment-processor-fallback:8080', true, 100)
ON CONFLICT (url)
DO NOTHING;
