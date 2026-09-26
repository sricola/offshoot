-- golden.sql: seed data and the one correct migration for the pass^k
-- example (examples/eval-pass-k/run.py). Two sections, split by a marker
-- comment line (search run.py's load_migrations for the exact string) that
-- appears exactly once below, right before the migration itself.

-- === SEED ===
--
-- Five independent tasks, each asking a stub "agent" to fill in one
-- order's total: quantity * unit_price * (1 - discount_pct), rounded to
-- the cent. Every unit_price below carries a fractional cent (4.995,
-- 249.995, 2.995, 89.999) on purpose -- forgetting to round produces a
-- value that differs from the correct rounded total on every single one
-- of them, so a buggy attempt is never accidentally "correct by luck".

CREATE TABLE tasks (
    id          INTEGER PRIMARY KEY,
    order_id    INTEGER NOT NULL,
    description TEXT NOT NULL
);

CREATE TABLE orders (
    id           INTEGER PRIMARY KEY,
    quantity     INTEGER NOT NULL,
    unit_price   REAL NOT NULL,
    discount_pct REAL NOT NULL,
    total        REAL
);

INSERT INTO tasks (id, order_id, description) VALUES
    (0, 1, 'Fill in order 1''s total, rounded to the cent.'),
    (1, 2, 'Fill in order 2''s total, rounded to the cent.'),
    (2, 3, 'Fill in order 3''s total, rounded to the cent.'),
    (3, 4, 'Fill in order 4''s total, rounded to the cent.'),
    (4, 5, 'Fill in order 5''s total, rounded to the cent.');

INSERT INTO orders (id, quantity, unit_price, discount_pct, total) VALUES
    (1, 3,  19.99,   0.10, NULL),
    (2, 7,  4.995,   0.00, NULL),
    (3, 1,  249.995, 0.15, NULL),
    (4, 12, 2.995,   0.05, NULL),
    (5, 2,  89.999,  0.00, NULL);

-- === MIGRATION ===
--
-- The correct fix. run.py applies this verbatim to build the golden
-- branch's "expected" checkpoint, and derives the deterministic bug (the
-- one a per-trial stub agent occasionally makes) by stripping the ROUND()
-- wrapper via a regex over this exact text -- the buggy variant is never
-- hand-duplicated, so it can't silently drift from the correct one.

UPDATE orders SET total = ROUND(quantity * unit_price * (1 - discount_pct), 2);
