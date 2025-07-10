-- Migration 006: Rollback promo_orders table structure changes
BEGIN;

-- Отменяем переименование поля order_amount обратно в revenue (если колонка order_amount существует)
DO $$
BEGIN
    IF EXISTS(SELECT * FROM information_schema.columns WHERE table_name='promo_orders' AND column_name='order_amount') THEN
        ALTER TABLE promo_orders RENAME COLUMN order_amount TO revenue;
    END IF;
END $$;

-- Удаляем добавленные индексы
DROP INDEX IF EXISTS idx_promo_orders_order_date;
DROP INDEX IF EXISTS idx_promo_orders_country;
DROP INDEX IF EXISTS idx_promo_orders_promo_source;

-- Удаляем добавленные колонки
ALTER TABLE promo_orders
    DROP COLUMN IF EXISTS transaction_id,
    DROP COLUMN IF EXISTS promo_source,
    DROP COLUMN IF EXISTS country,
    DROP COLUMN IF EXISTS is_processed;

-- Восстанавливаем ограничение уникальности по order_id
CREATE UNIQUE INDEX promo_orders_order_id_key ON promo_orders(order_id);

COMMIT;
