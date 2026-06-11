CREATE TABLE orders (
  id            SERIAL PRIMARY KEY,
  customer_name TEXT        NOT NULL,
  item          TEXT        NOT NULL,
  quantity      INT         NOT NULL DEFAULT 1,
  status        TEXT        NOT NULL DEFAULT 'pending',
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
  id          SERIAL PRIMARY KEY,
  name        TEXT        NOT NULL,
  price_cents INT         NOT NULL,
  stock       INT         NOT NULL DEFAULT 0,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO products (name, price_cents, stock) VALUES
  ('Widget A', 999,  100),
  ('Widget B', 1499, 50),
  ('Gadget X', 2999, 25);

INSERT INTO orders (customer_name, item, quantity, status) VALUES
  ('Alice', 'Widget A', 2, 'pending'),
  ('Bob',   'Gadget X', 1, 'shipped');
