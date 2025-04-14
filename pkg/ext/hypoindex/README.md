# hypoindex: Hypothetical Indexes Extension for CockroachDB

This extension enables users to experiment with hypothetical indexes without actually creating them in the database. It's similar to PostgreSQL's `hypopg` extension.

## Core Features

1. Create hypothetical index definitions stored in a system table
2. Generate EXPLAIN plans showing how queries would perform with these hypothetical indexes
3. Compare query costs with and without hypothetical indexes
4. Provide recommendations for which indexes to create

## Architecture

The extension integrates with CockroachDB's query optimizer and uses the existing hypothetical table infrastructure:

1. **Storage Layer**: Tables in the `pg_extension` schema store hypothetical index definitions
2. **API Layer**: SQL functions for managing hypothetical indexes
3. **Optimizer Integration**: Connects to CockroachDB's optimizer using `indexrec.HypotheticalTable`
4. **Explain Implementation**: Shows query plans with hypothetical indexes

## Implementation Details

### Core Components

1. **HypoIndexExplainer**: Main component that:
   - Parses SQL queries
   - Retrieves hypothetical index definitions
   - Creates hypothetical tables with those indexes
   - Runs the optimizer with these tables
   - Formats the EXPLAIN output

2. **SQL Functions**:
   - `hypo_create_index()`: Creates a hypothetical index definition
   - `hypo_drop_index()`: Removes a hypothetical index
   - `hypo_drop_all_indexes()`: Clears all hypothetical indexes
   - `hypo_list_indexes()`: Lists all defined hypothetical indexes
   - `hypo_explain()`: Executes EXPLAIN with hypothetical indexes

3. **Integration with CockroachDB**:
   - Uses parser to analyze SQL queries
   - Leverages `indexrec.BuildOptAndHypTableMaps` to create hypothetical tables
   - Integrates with the optimizer to estimate query costs

## Usage Examples

### Creating a Hypothetical Index

```sql
SELECT pg_extension.hypo_create_index(
  'public',        -- schema
  'users',         -- table
  'idx_users_name', -- index name
  ARRAY['name'],   -- indexed columns
  ARRAY['email']   -- stored columns
);
```

### Explaining a Query with Hypothetical Indexes

```sql
SELECT pg_extension.hypo_explain('
  SELECT * FROM users WHERE name = ''John''
');
```

### Dropping a Hypothetical Index

```sql
SELECT pg_extension.hypo_drop_index('index_id_uuid');
```

## Future Improvements

1. **Enhanced Integration**: More complete integration with the CockroachDB extension system
2. **Better Cost Estimation**: More accurate cost models for hypothetical indexes
3. **Index Recommendations**: Automated recommendations based on workload analysis
4. **Statistics**: Support for specifying hypothetical statistics for more accurate planning

## Development Status

This extension is under active development and ready for testing. It integrates with CockroachDB's optimizer and provides meaningful insights into how indexes would affect query performance.

## Contributing

Contributions are welcome! See the `IMPLEMENTATION.md` file for details on the internal architecture and implementation approach.

## License

See CockroachDB Software License in the /LICENSE file. 
