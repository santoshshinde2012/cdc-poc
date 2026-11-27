-- Mixed OLTP workload. Each loop iteration is its own transaction (COMMIT inside a procedure).
CREATE OR REPLACE PROCEDURE public.run_workload(n int, seed float8 DEFAULT 0.42)
LANGUAGE plpgsql AS $$
DECLARE
  i   int;
  r   float8;
  oid int;
BEGIN
  PERFORM setseed(seed);
  FOR i IN 1..n LOOP
    r := random();
    IF r < 0.20 THEN
      -- new customer (an id freed by a delete can come back: delete then re-insert)
      INSERT INTO customers (id, name, email)
      SELECT coalesce(max(id), 0) + 1,
             'customer ' || (coalesce(max(id), 0) + 1),
             'new' || (coalesce(max(id), 0) + 1) || '@example.com'
      FROM customers;
    ELSIF r < 0.45 THEN
      UPDATE customers
         SET tier = CASE WHEN tier = 'gold' THEN 'basic' ELSE 'gold' END,
             email = 'u' || i || '.' || id || '@example.com',
             updated_at = clock_timestamp()
       WHERE id = (SELECT id FROM customers ORDER BY random() LIMIT 1);
    ELSIF r < 0.55 THEN
      -- cascades to that customer's orders, so orders get delete events too
      DELETE FROM customers WHERE id = (SELECT id FROM customers ORDER BY random() LIMIT 1);
    ELSIF r < 0.75 THEN
      INSERT INTO orders (id, customer_id, amount)
      SELECT (SELECT coalesce(max(id), 0) + 1 FROM orders),
             (SELECT id FROM customers ORDER BY random() LIMIT 1),
             round((random() * 500)::numeric, 2);
    ELSIF r < 0.92 THEN
      -- two updates of the same row in one transaction: order inside a txn must hold
      SELECT id INTO oid FROM orders ORDER BY random() LIMIT 1;
      UPDATE orders SET status = 'paid', updated_at = clock_timestamp() WHERE id = oid;
      UPDATE orders SET status = 'shipped', amount = amount + 1, updated_at = clock_timestamp() WHERE id = oid;
    ELSE
      DELETE FROM orders WHERE id = (SELECT id FROM orders ORDER BY random() LIMIT 1);
    END IF;
    COMMIT;
  END LOOP;
END $$;
