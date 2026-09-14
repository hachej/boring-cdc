\set ON_ERROR_STOP on
-- This is the business mutation sequence from m1-workload-v1 (workload-v1).
BEGIN;
INSERT INTO customers VALUES (1, 'Ada', 1);
COMMIT;
BEGIN;
INSERT INTO products VALUES (10, 'P10', 12.50);
COMMIT;
BEGIN;
INSERT INTO orders VALUES (100, 1, 'new');
COMMIT;
BEGIN;
INSERT INTO order_items VALUES (100, 1, 10, 1);
COMMIT;
BEGIN;
UPDATE customers SET tier = 2 WHERE id = 1;
UPDATE customers SET tier = 3 WHERE id = 1;
COMMIT;
BEGIN;
DELETE FROM order_items WHERE order_id = 100 AND line_no = 1;
COMMIT;
BEGIN;
INSERT INTO order_items VALUES (100, 1, 10, 2);
COMMIT;
BEGIN;
DELETE FROM customers WHERE id = 1;
INSERT INTO customers VALUES (2, 'Ada', 3);
COMMIT;

-- Stable row-shape output for reset/replay checks.
SELECT 'customers' AS table_name, id::text AS key, name || '|' || tier AS row_value FROM customers
UNION ALL SELECT 'products', id::text, sku || '|' || price FROM products
UNION ALL SELECT 'orders', id::text, customer_id || '|' || status FROM orders
UNION ALL SELECT 'order_items', order_id || ':' || line_no, product_id || '|' || quantity FROM order_items
ORDER BY 1, 2;
