CREATE TABLE users (
    id bigserial primary key,
    name text not null,
    email text not null,
    created_date timestamptz not null default now()
);

CREATE TABLE orders (
    id bigserial primary key,
    user_id bigint not null references users(id)
);
