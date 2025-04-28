# Implementation Design for `hypo_index_explain`

## Overview
This document outlines the design and implementation plan for the `hypo_index_explain` function in CockroachDB. This feature simulates the behavior of PostgreSQL's HypoPG extension, allowing users to see the execution plan for a query as if hypothetical indexes existed without actually creating them.

## Background
In PostgreSQL, the HypoPG extension (https://github.com/HypoPG/hypopg) allows users to create hypothetical indexes that the query planner can use to generate query plans without materializing the indexes. This helps database administrators evaluate different indexing strategies without affecting database performance or making actual schema changes.

CockroachDB already has infrastructure for index recommendations and hypothetical indexes in the context of the index recommendation system, which we can leverage for this feature.

## Design Goals
1. Provide a function that accepts a SQL query and a list of hypothetical indexes
2. Generate an EXPLAIN plan that shows how the query would be executed if the hypothetical indexes existed
3. Ensure the function is easy to use and provides meaningful results
4. Leverage existing CockroachDB infrastructure for hypothetical indexes
5. Not generate additional index recommendations in the EXPLAIN output

## Function Interface
```sql
SELECT hypo_index_explain(
  'SELECT * FROM schema.table1 JOIN schema.table2 ON table1.id = table2.id WHERE col1 > 10 AND col3 < 100',
  ARRAY[
    'CREATE INDEX hypo_idx1 ON schema.table1 (col1, col2)',
    'CREATE INDEX hypo_idx2 ON schema.table2 (col3) STORING (col4)'
  ],
  'VERBOSE'  -- Optional parameter for EXPLAIN options
);
```

The function signature is:
```
hypo_index_explain(query STRING, indexes STRING[], options STRING) -> STRING
```

Where:
- `query` is the SQL query to be explained
- `indexes` is an array of CREATE INDEX statements defining hypothetical indexes
- `options` (optional) is a string of space-separated EXPLAIN options (e.g., 'VERBOSE', 'TYPES')

## Implementation Approach

The implementation has been carried out in phases:

### Phase 1: Function Definition and Basic Implementation
- Define and register the `hypo_index_explain` function in the SQL builtin catalog
- Create a stub implementation that returns a constant string
- Add comprehensive error handling for invalid input parameters
- Set up basic testing infrastructure

### Phase 2: Integration with Optimizer
- Integrate with the optimizer by using the optimizer infrastructure
- Convert the input CREATE INDEX statements into hypothetical indexes
- Generate the EXPLAIN output for the query with these hypothetical indexes
- Format and return the results

### Phase 3: Additional Features and Polishing
- Add support for various EXPLAIN options
- Improve output formatting
- Add additional error handling
- Add performance optimizations

## Detailed Implementation

### Function Registration and Entry Point
The function is registered in `pkg/sql/sem/builtins/hypo_index.go` with two overloads:
1. `hypo_index_explain(query STRING, indexes STRING[]) -> STRING`
2. `hypo_index_explain(query STRING, indexes STRING[], options STRING) -> STRING`

The function performs parameter validation and then calls the planner's `HypoIndexExplainBuiltin` method.

### Core Implementation
The main implementation is in `pkg/sql/hypo_index_explain.go` and consists of:

1. `HypoIndexExplainBuiltin`: The entry point from the builtin function that:
   - Parses the query and CREATE INDEX statements
   - Validates input parameters
   - Calls `makeQueryPlanWithHypotheticalIndexesOpt` to generate the plan
   - Formats the final output

2. `makeQueryPlanWithHypotheticalIndexesOpt`: The core function that:
   - Initializes a temporary optimizer
   - Builds the memo for the query
   - Converts CREATE INDEX statements to hypothetical index candidates
   - Uses `indexrec.BuildOptAndHypTableMaps` to create hypothetical tables with indexes
   - Re-optimizes the query with hypothetical indexes
   - Generates a formatted explain plan

3. `formatExplainPlan`: Formats the explain output to include:
   - Basic plan type and cost information
   - Table and index details, marking hypothetical indexes
   - Selected plan details

### Error Handling
The implementation includes comprehensive error handling:
- Validation of SQL query syntax
- Validation of CREATE INDEX statement syntax
- Checking if referenced tables exist
- Recovery from optimizer panics
- Graceful handling of various edge cases

### Output Format
The output includes:
- A header indicating the query and hypothetical indexes provided
- The execution plan using hypothetical indexes
- Information about the optimizer's selected plan
- Table and index details, clearly marking hypothetical indexes

## Implementation Progress

### Completed
- Function definition and registration
- Input parameter validation
- Parsing of SQL query and CREATE INDEX statements
- Integration with optimizer
- Generation of EXPLAIN output
- Comprehensive error handling
- Testing infrastructure

### Current Status
The `hypo_index_explain` function is fully implemented and supports:
- Evaluating single or multiple hypothetical indexes on a query
- Detailed output showing how hypothetical indexes would be used
- Index definitions using standard CREATE INDEX syntax
- Error reporting for invalid inputs or non-existent tables

### Future Enhancements
- Support for more complex index options (e.g., partial indexes, expression indexes)
- More detailed cost comparison between different indexing strategies
- Visual comparison of execution plans
- Session-level API for managing hypothetical indexes
- More sophisticated explain options

## Conclusion
The `hypo_index_explain` function provides a powerful tool for database administrators to evaluate different indexing strategies without the overhead of actually creating indexes. By leveraging the existing optimizer and hypothetical index infrastructure, it provides accurate and meaningful execution plans that can inform database design decisions. 
