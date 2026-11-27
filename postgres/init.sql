-- Demo source schema for the push-based CDC proof of concept.
CREATE TABLE public.customers (
  id          integer PRIMARY KEY,
  name        text        NOT NULL,
  email       text        NOT NULL,
  tier        text        NOT NULL DEFAULT 'basic',
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.orders (
  id           integer PRIMARY KEY,
  customer_id  integer       NOT NULL REFERENCES public.customers(id) ON DELETE CASCADE,
  amount       numeric(10,2) NOT NULL,
  status       text          NOT NULL DEFAULT 'new',
  updated_at   timestamptz   NOT NULL DEFAULT now()
);

-- Not captured. Used to generate WAL that the slot must keep while Debezium is stopped.
CREATE TABLE public.noise (
  id      bigserial PRIMARY KEY,
  payload text
);

-- Old values of every column on update and delete.
ALTER TABLE public.customers REPLICA IDENTITY FULL;
ALTER TABLE public.orders    REPLICA IDENTITY FULL;

-- Created up front, Debezium runs with publication.autocreate.mode=disabled.
CREATE PUBLICATION dbz_publication FOR TABLE public.customers, public.orders;

-- Initial rows, these arrive as snapshot (op = 'r') events.
INSERT INTO public.customers (id, name, email, tier)
SELECT g, 'customer ' || g, 'c' || g || '@example.com', CASE WHEN g % 5 = 0 THEN 'gold' ELSE 'basic' END
FROM generate_series(1, 50) g;

INSERT INTO public.orders (id, customer_id, amount, status)
SELECT g, 1 + (g % 50), round((10 + g * 1.37)::numeric, 2), 'new'
FROM generate_series(1, 100) g;
