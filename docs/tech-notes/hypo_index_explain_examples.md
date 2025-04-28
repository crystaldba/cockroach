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

The output will look something like:

```
# EXPLAIN with hypothetical indexes
# Query: SELECT * FROM orders WHERE order_date > '2023-04-15'
# Hypothetical indexes provided:
#   1: CREATE INDEX idx_order_date ON orders(order_date)

# Execution Plan:
Optimizer plan with hypothetical indexes:

Optimizer Cost: 9.78

Plan Type: *memo.ScanExpr
Tables in query: 1

Table: test.public.orders, Indexes: 3
  Index 0: primary (id, customer_id, order_date, amount, status, ...)
  Index 1: idx_orders_customer (customer_id, id, order_date, amount, status, ...)
  Index 2: _hyp_1 (hypothetical) (order_date, id, customer_id, amount, status, ...)

Optimizer's Selected Plan:

Note: This execution plan shows the optimizer's plan using hypothetical indexes.
The plan cost and structure reflect how the query would be executed if these indexes existed.
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

This will output a plan showing which of the hypothetical indexes would be selected by the optimizer:

```
# EXPLAIN with hypothetical indexes
# Query: SELECT * FROM orders WHERE order_date > '2023-04-15' AND status = 'completed'
# Hypothetical indexes provided:
#   1: CREATE INDEX idx_order_date ON orders(order_date)
#   2: CREATE INDEX idx_status ON orders(status)
#   3: CREATE INDEX idx_date_status ON orders(order_date, status)

# Execution Plan:
Optimizer plan with hypothetical indexes:

Optimizer Cost: 7.52

Plan Type: *memo.SelectExpr
Tables in query: 1

Table: test.public.orders, Indexes: 5
  Index 0: primary (id, customer_id, order_date, amount, status, ...)
  Index 1: idx_orders_customer (customer_id, id, order_date, amount, status, ...)
  Index 2: _hyp_1 (hypothetical) (order_date, id, customer_id, amount, status, ...)
  Index 3: _hyp_2 (hypothetical) (status, id, customer_id, order_date, amount, ...)
  Index 4: _hyp_3 (hypothetical) (order_date, status, id, customer_id, amount, ...)

Optimizer's Selected Plan:

Note: This execution plan shows the optimizer's plan using hypothetical indexes.
The plan cost and structure reflect how the query would be executed if these indexes existed.
```

## Using Different EXPLAIN Options

You can also specify additional EXPLAIN options (support for this may vary):

```sql
-- With verbose output
SELECT hypo_index_explain(
    'SELECT * FROM orders WHERE order_date > ''2023-04-15''',
    ARRAY['CREATE INDEX idx_order_date ON orders(order_date)'],
    'VERBOSE'
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

The output will show how the optimizer would use the hypothetical indexes in a join:

```
# EXPLAIN with hypothetical indexes
# Query: SELECT c.name, o.order_date, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id WHERE c.city = 'New York' AND o.amount > 100
# Hypothetical indexes provided:
#   1: CREATE INDEX idx_city ON customers(city)
#   2: CREATE INDEX idx_amount ON orders(amount)

# Execution Plan:
Optimizer plan with hypothetical indexes:

Optimizer Cost: 11.87

Plan Type: *memo.ProjectExpr
Tables in query: 2

Table: test.public.customers, Indexes: 2
  Index 0: primary (id, name, email, city, state, signup_date, ...)
  Index 1: _hyp_1 (hypothetical) (city, id, name, email, state, signup_date, ...)

Table: test.public.orders, Indexes: 3
  Index 0: primary (id, customer_id, order_date, amount, status, ...)
  Index 1: idx_orders_customer (customer_id, id, order_date, amount, status, ...)
  Index 2: _hyp_2 (hypothetical) (amount, id, customer_id, order_date, status, ...)

Optimizer's Selected Plan:

Note: This execution plan shows the optimizer's plan using hypothetical indexes.
The plan cost and structure reflect how the query would be executed if these indexes existed.
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

## Error Handling

The function provides helpful error messages for common issues:

```sql
-- Non-existent table
SELECT hypo_index_explain(
    'SELECT * FROM orders WHERE order_date > ''2023-04-15''',
    ARRAY['CREATE INDEX idx_fake ON non_existent_table (col1)']
);

-- Output:
-- # EXPLAIN with hypothetical indexes
-- # Query: SELECT * FROM orders WHERE order_date > '2023-04-15'
-- # Hypothetical indexes provided:
-- # Warning: Table 'non_existent_table' not found in the database
--
-- # Execution Plan:
-- No plan available - some tables in the index definitions do not exist.
```

## Conclusion

The `hypo_index_explain` function is a powerful tool for exploring different indexing strategies without the overhead of actually creating the indexes. By evaluating the impact of hypothetical indexes on query plans, you can make more informed decisions about which indexes to create in your database. 

When interpreting the results, pay attention to:
1. Which hypothetical index was selected by the optimizer
2. The estimated cost with the hypothetical index
3. How the plan structure changes with different indexing strategies
