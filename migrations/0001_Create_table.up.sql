CREATE SCHEMA myschema;

CREATE TABLE myschema.test (
    id bigserial primary key
);

CREATE TABLE users (
    id bigserial primary key,
    name text not null,
    email text not null,
    created_date timestamptz not null default now(),
    something bigint references myschema.test(id)
);

CREATE TABLE orders (
    id bigserial primary key,
    user_id bigint not null references users(id),
    unique (id, user_id)
);

CREATE TABLE payments (
    user_id bigint not null,
    order_id bigint not null,
    FOREIGN KEY (user_id, order_id) REFERENCES orders(id, user_id)
);

ALTER TABLE payments drop constraint payments_user_id_order_id_fkey;

ALTER TABLE users
    ADD COLUMN abcd text not null,
    DROP COLUMN something;

ALTER TABLE orders
    DROP COLUMN user_id cascade;