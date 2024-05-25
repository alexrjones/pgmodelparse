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

-- ALTER TABLE users DROP CONSTRAINT fk