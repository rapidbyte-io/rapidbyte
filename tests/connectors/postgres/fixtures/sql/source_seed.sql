CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com'),
    ('Carol', 'carol@example.com');

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    amount_cents INTEGER NOT NULL,
    status TEXT DEFAULT 'pending'
);

INSERT INTO orders (user_id, amount_cents, status) VALUES
    (1, 5000, 'completed'),
    (2, 12000, 'pending'),
    (1, 3500, 'completed');
