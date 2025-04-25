# Hypo Index Explain: Usage Examples

This document provides examples of how to use the `hypo_index_explain` function to evaluate hypothetical indexes without actually creating them.

## Setup

First, let's create some example tables and data:

```sql
CREATE TABLE customers (
    id INT PRIMARY KEY,
    name STRING,
    email STRING,
    city STRING,
    state STRING,
    signup_date DATE
);

CREATE TABLE orders (
    id INT PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    order_date DATE,
    amount DECIMAL,
    status STRING
);

-- Add a basic index to orders
CREATE INDEX idx_orders_customer ON orders(customer_id);

-- Insert some sample data
INSERT INTO customers (id, name, email, city, state, signup_date)
VALUES
    (1, 'Alice Smith', 'alice@example.com', 'New York', 'NY', '2023-01-15'),
    (2, 'Bob Johnson', 'bob@example.com', 'Chicago', 'IL', '2023-02-20'),
    (3, 'Carol Lee', 'carol@example.com', 'San Francisco', 'CA', '2023-03-10');

INSERT INTO orders (id, customer_id, order_date, amount, status)
VALUES
    (101, 1, '2023-04-01', 150.00, 'completed'),
    (102, 1, '2023-04-15', 75.50, 'completed'),
    (103, 2, '2023-04-20', 200.00, 'pending'),
    (104, 3, '2023-04-25', 120.75, 'completed');
```

## Basic Usage

Now, let's see how we can use `hypo_index_explain` to evaluate a query:

```sql
-- Original query without hypothetical index
EXPLAIN SELECT * FROM orders WHERE order_date > '2023-04-15';

-- Same query with a hypothetical index on order_date
SELECT hypo_index_explain(
    'SELECT * FROM orders WHERE order_date > ''2023-04-15''',
    ARRAY['CREATE INDEX idx_order_date ON orders(order_date)']
);
```

## Multiple Hypothetical Indexes

You can evaluate multiple hypothetical indexes at once:

```sql
-- Query with multiple filter conditions
SELECT hypo_index_explain(
    'SELECT * FROM orders WHERE order_date > ''2023-04-15'' AND status = ''completed''',
    ARRAY[
        'CREATE INDEX idx_order_date ON orders(order_date)',
        'CREATE INDEX idx_status ON orders(status)',
        'CREATE INDEX idx_date_status ON orders(order_date, status)'
    ]
);
```

## Using Different EXPLAIN Options

You can also specify different EXPLAIN options:

```sql
-- With verbose output
SELECT hypo_index_explain(
    'SELECT * FROM orders WHERE order_date > ''2023-04-15''',
    ARRAY['CREATE INDEX idx_order_date ON orders(order_date)'],
    'VERBOSE'
);

-- With optimizer output
SELECT hypo_index_explain(
    'SELECT * FROM orders WHERE order_date > ''2023-04-15''',
    ARRAY['CREATE INDEX idx_order_date ON orders(order_date)'],
    'OPT'
);
```

## Evaluating Joins

You can also evaluate hypothetical indexes for join queries:

```sql
-- Join query with hypothetical indexes
SELECT hypo_index_explain(
    'SELECT c.name, o.order_date, o.amount 
     FROM customers c 
     JOIN orders o ON c.id = o.customer_id 
     WHERE c.city = ''New York'' AND o.amount > 100',
    ARRAY[
        'CREATE INDEX idx_city ON customers(city)',
        'CREATE INDEX idx_amount ON orders(amount)'
    ]
);
```

## Unique and Composite Indexes

You can evaluate unique and composite indexes:

```sql
-- Unique index
SELECT hypo_index_explain(
    'SELECT * FROM customers WHERE email = ''alice@example.com''',
    ARRAY['CREATE UNIQUE INDEX idx_email ON customers(email)']
);

-- Composite index with STORING columns
SELECT hypo_index_explain(
    'SELECT name, email, city, state FROM customers WHERE city = ''New York'' AND state = ''NY''',
    ARRAY['CREATE INDEX idx_location ON customers(city, state) STORING (name, email)']
);
```

## Comparing Different Indexing Strategies

You can use this function to compare different indexing strategies:

```sql
-- Strategy 1: Single-column indexes
SELECT hypo_index_explain(
    'SELECT * FROM orders WHERE customer_id = 1 AND order_date > ''2023-04-15''',
    ARRAY[
        'CREATE INDEX idx_customer ON orders(customer_id)',
        'CREATE INDEX idx_date ON orders(order_date)'
    ]
);

-- Strategy 2: Composite index
SELECT hypo_index_explain(
    'SELECT * FROM orders WHERE customer_id = 1 AND order_date > ''2023-04-15''',
    ARRAY['CREATE INDEX idx_customer_date ON orders(customer_id, order_date)']
);

-- Strategy 3: Composite index with different column order
SELECT hypo_index_explain(
    'SELECT * FROM orders WHERE customer_id = 1 AND order_date > ''2023-04-15''',
    ARRAY['CREATE INDEX idx_date_customer ON orders(order_date, customer_id)']
);
```

## Conclusion

The `hypo_index_explain` function is a powerful tool for exploring different indexing strategies without the overhead of actually creating the indexes. By evaluating the impact of hypothetical indexes on query plans, you can make more informed decisions about which indexes to create in your database. 
