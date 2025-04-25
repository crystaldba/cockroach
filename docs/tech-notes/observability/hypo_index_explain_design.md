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

The implementation is being carried out in phases:

### Phase 1: Function Definition and Basic Implementation
- Define and register the `hypo_index_explain` function in the SQL builtin catalog
- Create a stub implementation that returns a constant string
- Add comprehensive error handling for invalid input parameters
- Set up basic testing infrastructure

### Phase 2: Integration with Optimizer
- Integrate with the optimizer by calling `makeQueryIndexRecommendation` (in `plan_opt.go`)
- Convert the input CREATE INDEX statements into hypothetical indexes
- Generate the EXPLAIN output for the query with these hypothetical indexes
- Format and return the results

### Phase 3: Additional Features and Polishing
- Add support for various EXPLAIN options
- Improve output formatting
- Add additional error handling
- Add performance optimizations

## Detailed Implementation Steps

### Step 1: Function Definition and Basic Implementation (Phase 1)
- Create a new file `pkg/sql/sem/builtins/hypo_index.go`
- Implement `hypoIndexExplain` function that accepts a SQL query and array of CREATE INDEX statements
- Register the function in `pkg/sql/sem/builtins/builtins.go`
- Add basic validation for input parameters
- Return a constant string indicating a successful call with the provided parameters
- Create test files and add basic tests for the function

### Step 2: Integration with Optimizer (Phase 2)
For this phase, we will:

1. Create a new `pkg/sql/hypo_index_explain.go` file to implement the planner-level logic
2. Create a new planner method `HypoIndexExplainBuiltin` that will be called from our builtin function
3. Adapt the logic from `makeQueryIndexRecommendation` in `pkg/sql/plan_opt.go` to work with our custom hypothetical indexes
4. Use the existing `indexrec.BuildOptAndHypTableMaps` infrastructure to create hypothetical tables with the indexes
5. Generate the EXPLAIN output for the query with these hypothetical indexes

The implementation will look like:
```go
// In pkg/sql/hypo_index_explain.go
func (p *planner) HypoIndexExplainBuiltin(
    ctx context.Context, 
    indexes tree.Datum,
    query tree.Datum,
    options tree.Datum,
) (tree.Datum, error) {
    // Parse the input query
    // Parse the CREATE INDEX statements
    // Convert CREATE INDEX statements to hypothetical indexes
    // Call makeQueryIndexRecommendation-like logic with these hypothetical indexes
    // Generate EXPLAIN output
    // Return the formatted result
}

// In pkg/sql/sem/builtins/hypo_index.go
func hypoIndexExplain(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
    // Basic parameter validation
    // Call p.HypoIndexExplainBuiltin with the validated parameters
    // Return the result
}
```

### Step 3: Additional Features and Testing (Phase 3)
- Improve the output formatting to match PostgreSQL's EXPLAIN output style
- Add support for various EXPLAIN options (VERBOSE, TYPES, etc.)
- Add comprehensive error handling for edge cases
- Optimize performance for large queries or multiple indexes
- Expand test coverage with more complex scenarios

## Limitations and Future Enhancements
1. Initial implementation might not support all index types or options
2. Future enhancements could include:
   - A session-level API for managing hypothetical indexes
   - Cost estimation comparisons between different index configurations
   - Integration with index recommendation system
   - Visual comparison of execution plans with different index combinations
   - Supporting more index types and options (e.g., partial indexes, expression indexes)

## Implementation Progress

### Completed
- Function definition and registration
- Input parameter validation
- Parsing of SQL query and CREATE INDEX statements
- Comprehensive error handling
- Test infrastructure setup

### In Progress
- Integration with optimizer
- Generation of actual EXPLAIN output

### Future Work
- Support for all EXPLAIN options
- User experience improvements
- Performance optimizations
- Additional index types and options

## Conclusion
The implementation is proceeding in well-defined phases. The first two phases (function definition and input validation) are complete, and we're now moving to the core phase of integrating with the optimizer to generate EXPLAIN plans using hypothetical indexes. This phased approach allows us to build up functionality incrementally while maintaining a solid foundation of validation and error handling. 
