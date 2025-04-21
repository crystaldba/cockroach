# CRDB Hypothetical Index Explain

- Feature Name: `hypo_index_explain`
- Status: Completed
- Start Date: 2024-04-12

## Summary

The `hypo_index_explain` function allows users to see what an execution plan would look like if hypothetical indexes existed, without actually creating the indexes. This is similar to PostgreSQL's [HypoPG extension](https://hypopg.readthedocs.io/en/latest/).

## Motivation

Database administrators and application developers often need to make decisions about index creation to optimize query performance. However, creating indexes on large tables is expensive in terms of time and resources. Additionally, each new index increases write amplification and storage overhead.

Therefore, it's valuable to be able to see how a query execution plan would change if certain indexes existed, without actually creating those indexes. This allows for experimentation and validation of indexing strategies before committing to them.

## Design

The `hypo_index_explain` function takes a SQL query, an array of hypothetical CREATE INDEX statements, and an optional EXPLAIN mode parameter. It then uses the optimizer to generate an execution plan as if those indexes existed, without actually creating them.

### Function Signature

```sql
hypo_index_explain(
    query STRING, 
    indexes STRING[], 
    options STRING DEFAULT 'tree'
) -> STRING
```

Where:
- `query` is the SQL query to explain
- `indexes` is an array of CREATE INDEX statements that define the hypothetical indexes
- `options` is a string containing EXPLAIN options (e.g., 'VERBOSE', 'OPT', etc.)

The function returns a string containing the EXPLAIN output for the query as if the hypothetical indexes existed.

### Implementation

The implementation involves:

1. Parsing and validating the input query and CREATE INDEX statements
2. Converting the CREATE INDEX statements into hypothetical index objects
3. Creating hypothetical table descriptors with these indexes
4. Using a modified catalog that returns these hypothetical table descriptors
5. Running the optimizer with this catalog to generate a query plan
6. Formatting and returning the explain output

## Examples

### Basic Usage

```sql
SELECT hypo_index_explain(
    'SELECT * FROM orders WHERE customer_id = 123',
    ARRAY['CREATE INDEX idx_customer_id ON orders(customer_id)']
);
```

### With Multiple Indexes

```sql
SELECT hypo_index_explain(
    'SELECT * FROM orders WHERE customer_id = 123 AND order_date > $1',
    ARRAY[
        'CREATE INDEX idx_customer_id ON orders(customer_id)',
        'CREATE INDEX idx_order_date ON orders(order_date)',
        'CREATE INDEX idx_customer_order ON orders(customer_id, order_date)'
    ]
);
```

### With EXPLAIN Options

```sql
SELECT hypo_index_explain(
    'SELECT * FROM orders WHERE customer_id = 123',
    ARRAY['CREATE INDEX idx_customer_id ON orders(customer_id)'],
    'VERBOSE'
);
```

## Limitations

The current implementation has the following limitations:

1. Inverted indexes are not supported
2. Partitioned indexes are not supported
3. Expression indexes are not supported (only simple column references)
4. The function requires appropriate permissions to access the tables in the query

## Future Enhancements

Potential future enhancements include:

1. Support for more index types (inverted, expression, partial)
2. Session-level API for managing hypothetical indexes
3. Better visualization of plan differences with vs. without hypothetical indexes
4. Integration with index recommendation system

## Conclusion

The `hypo_index_explain` function provides a valuable tool for database administrators and developers to experiment with different indexing strategies without actually creating the indexes, which can help optimize query performance with minimal overhead. 
