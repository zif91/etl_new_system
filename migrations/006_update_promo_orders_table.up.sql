-- Migration 006: Update promo_orders table structure
BEGIN;

-- Добавляем новые поля в таблицу promo_orders
ALTER TABLE promo_orders
    ADD COLUMN IF NOT EXISTS transaction_id TEXT UNIQUE,
    ADD COLUMN IF NOT EXISTS promo_source TEXT,
    ADD COLUMN IF NOT EXISTS country TEXT,
    ADD COLUMN IF NOT EXISTS is_processed BOOLEAN DEFAULT FALSE;

-- Обновляем ограничение уникальности
-- Сначала удаляем ограничение уникальности, если оно существует
ALTER TABLE promo_orders DROP CONSTRAINT IF EXISTS promo_orders_order_id_key;
-- Затем создаем новый индекс для transaction_id
CREATE UNIQUE INDEX IF NOT EXISTS promo_orders_transaction_id_key ON promo_orders(transaction_id);

-- Переименовываем поле revenue в order_amount для соответствия PRD (если колонка существует)
DO $$
BEGIN
    IF EXISTS(SELECT * FROM information_schema.columns WHERE table_name='promo_orders' AND column_name='revenue') THEN
        ALTER TABLE promo_orders RENAME COLUMN revenue TO order_amount;
    END IF;
END $$;

-- Создаем индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_promo_orders_order_date ON promo_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_promo_orders_country ON promo_orders(country);
CREATE INDEX IF NOT EXISTS idx_promo_orders_promo_source ON promo_orders(promo_source);

COMMIT;
