-- V1.1.0__add_orders_table.cql
-- Add orders table with user_id as partition key
-- so we can efficiently query all orders for a user

CREATE TABLE IF NOT EXISTS orders (
    user_id    uuid,
    order_id   uuid,
    status     text,
    total      decimal,
    created_at timestamp,
    PRIMARY KEY (user_id, order_id)
) WITH CLUSTERING ORDER BY (order_id DESC);
