\set ON_ERROR_STOP on

CREATE TABLE customers (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    tier integer NOT NULL
);
CREATE TABLE products (
    id bigint PRIMARY KEY,
    sku text NOT NULL,
    price numeric(12,2) NOT NULL
);
CREATE TABLE orders (
    id bigint PRIMARY KEY,
    customer_id bigint NOT NULL,
    status text NOT NULL
);
CREATE TABLE order_items (
    order_id bigint NOT NULL,
    line_no integer NOT NULL,
    product_id bigint NOT NULL,
    quantity integer NOT NULL,
    PRIMARY KEY (order_id, line_no)
);

-- Fixed reference rows coexist with the m1-workload-v1 mutation keys.
INSERT INTO customers VALUES (1000, 'Seed Customer', 1);
INSERT INTO products VALUES (1010, 'SEED-10', 12.50);
INSERT INTO orders VALUES (1100, 1000, 'seeded');
INSERT INTO order_items VALUES (1100, 1, 1010, 1);

CREATE PUBLICATION article1_publication
    FOR TABLE customers, products, orders, order_items
    WITH (publish = 'insert, update, delete');
SELECT slot_name, lsn
FROM pg_create_logical_replication_slot('article1_slot', 'pgoutput');
