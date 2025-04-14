# hypoindex: Hypothetical Indexes for CockroachDB

This extension provides functionality similar to PostgreSQL's `hypopg` extension, allowing users to experiment with hypothetical indexes without actually creating them in the database.

## Current Status

This is a minimalist implementation to demonstrate the concept. Due to the complexity of integrating with CockroachDB's internal query optimizer, this extension currently provides a simplified API.

## Features

The extension supports:

1. Storing hypothetical index definitions
2. Simulating EXPLAIN output with hypothetical indexes

## Building and Testing

### Building with Bazel

```bash
bazel build //pkg/ext/hypoindex
```

### Testing the Example

```bash
bazel run //pkg/ext/hypoindex/example
```

## Future Improvements

1. Connect to the internal query optimizer
2. Leverage the existing HypotheticalTable implementation
3. Provide accurate explain plans that truly reflect hypothetical index performance

## Architecture

The extension has a simple architecture:

1. **Storage Layer**: A table to store hypothetical index definitions
2. **API Layer**: Functions to create, list, and drop hypothetical indexes
3. **Explain Implementation**: A function to simulate query plans with hypothetical indexes

## Usage Example

```sql
-- Create a hypothetical index
SELECT pg_extension.hypo_create_index(
  'public',        -- schema name
  'users',         -- table name
  'idx_users_name', -- index name
  ARRAY['name'],   -- columns
  ARRAY['email']   -- storing columns (optional)
);

-- Run EXPLAIN with hypothetical indexes
SELECT pg_extension.hypo_explain('SELECT * FROM users WHERE name = ''John''');
```

## Overview

The `hypoindex` extension lets you:

1. Create hypothetical indexes that only exist for query planning
2. Run EXPLAIN plans to see how these hypothetical indexes would affect query performance
3. Get recommendations for which indexes to create based on workload analysis

## Installation

To install the extension:

```sql
CREATE EXTENSION hypoindex;
```

## Usage

### Creating a hypothetical index

```sql
SELECT pg_extension.hypo_create_index(
  'public',        -- schema name
  'users',         -- table name
  'idx_users_name', -- index name
  ARRAY['name'],   -- columns
  ARRAY['email']   -- storing columns (optional)
);
```

### Dropping a hypothetical index

```sql
SELECT pg_extension.hypo_drop_index('index_id_uuid');
```

### Dropping all hypothetical indexes

```sql
SELECT pg_extension.hypo_drop_all_indexes();
```

### Listing hypothetical indexes

```sql
SELECT * FROM pg_extension.hypo_list_indexes();
```

### Running EXPLAIN with hypothetical indexes

```sql
SELECT pg_extension.hypo_explain('SELECT * FROM users WHERE name = ''John''');
```

## Implementation Details

This extension leverages CockroachDB's internal query optimizer and index recommendation system to simulate hypothetical indexes. The implementation:

1. Creates a table to store hypothetical index definitions
2. Provides SQL functions for creating, listing, and dropping hypothetical indexes
3. Implements a custom function to run EXPLAIN with hypothetical indexes by:
   - Creating hypothetical tables with the defined indexes
   - Running the optimizer with these hypothetical tables
   - Returning the resulting explain plan

## Limitations

- The extension cannot perfectly predict the exact performance impact of indexes
- Some complex index types may not be fully supported
- Results are estimations based on the optimizer's cost model

## Development Status

This extension is a work in progress and is not yet production-ready.

## License

See CockroachDB Software License in the /LICENSE file. 
