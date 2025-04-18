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

### Phase 1: Function Definition and Registration (Completed)
- Define and register the `hypo_index_explain` function in the SQL builtin catalog
- Create a stub implementation that returns a constant string

### Phase 2: Input Parsing and Validation (Completed)
- Validate and parse the input SQL query
- Parse and validate each CREATE INDEX statement
- Validate the optional EXPLAIN options
- Return a formatted string with the parsed information

### Phase 3: Integration with Optimizer (Next Step)
- Create hypothetical index objects from the validated CREATE INDEX statements
- Integrate with the optimizer to generate an execution plan that uses the hypothetical indexes
- Generate the EXPLAIN output

### Phase 4: Additional Features and Polishing
- Add support for various EXPLAIN options
- Improve output formatting
- Add additional error handling
- Add performance optimizations

## Detailed Implementation Steps

### Step 1: Function Registration (Completed)
- Created a new file `pkg/sql/sem/builtins/hypo_index.go`
- Implemented `hypoIndexExplain` function
- Registered the function in `pkg/sql/sem/builtins/builtins.go`

### Step 2: Input Parsing and Validation (Completed)
- Added validation for all input parameters
- Implemented parsing of SQL query using `parser.Parse`
- Implemented parsing and validation of each CREATE INDEX statement
- Added comprehensive error handling for various edge cases including:
  - Invalid SQL query
  - Invalid or non-CREATE INDEX statements
  - Empty index array
  - Multiple statements in a single query or index definition

### Step 3: Integration with Optimizer (Next Step)
For this phase, we need to:

1. Create hypothetical indexes from the parsed statements:
```go
// Build hypothetical indexes from parsed CREATE INDEX statements
func buildHypotheticalIndexes(
    ctx context.Context,
    evalCtx *eval.Context, 
    parsedIndexes []createIndexStmt,
) (map[catalog.TableDescriptor][]indexrec.IndexCandidate, error) {
    // Convert each parsed CREATE INDEX statement into an indexrec.IndexCandidate
    // This will require:
    // 1. Resolving the target table
    // 2. Creating column information for index columns and STORING columns
    // 3. Setting up index properties (uniqueness, invisibility, etc.)
}
```

2. Use the optimizer with hypothetical indexes:
```go
// Generate EXPLAIN plan with hypothetical indexes
func generateExplainWithHypotheticalIndexes(
    ctx context.Context,
    p *planner,
    stmt tree.Statement, 
    indexes map[catalog.TableDescriptor][]indexrec.IndexCandidate,
    explainOptions string,
) (string, error) {
    // Create an optimizer context
    var opc optPlanningCtx
    opc.init(ctx, p)
    
    // Using similar approach to makeQueryIndexRecommendation:
    // 1. Save the normalized memo
    // 2. Create hypothetical tables with our indexes
    // 3. Optimize the query with these hypothetical tables
    // 4. Generate EXPLAIN output
    // 5. Ensure index recommendations are disabled during optimization
}
```

3. Update the main function to use these components:
```go
func hypoIndexExplain(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
    // Initial validation and parsing (completed)
    
    // Build hypothetical indexes
    hypIndexes, err := buildHypotheticalIndexes(ctx, evalCtx, parsedIndexes)
    if err != nil {
        return nil, err
    }
    
    // Generate EXPLAIN plan
    explainOutput, err := generateExplainWithHypotheticalIndexes(
        ctx, evalCtx.Planner, parsedQuery, hypIndexes, explainOptions)
    if err != nil {
        return nil, err
    }
    
    return tree.NewDString(explainOutput), nil
}
```

The core challenge in this phase will be correctly adapting the code from `makeQueryIndexRecommendation` in `pkg/sql/plan_opt.go` to work with our custom hypothetical indexes rather than using automatically generated index candidates.

We need to:
1. Create a new `pkg/sql/hypo_index_explain.go` file to implement the planner-level logic
2. Create a new planner method `HypoIndexExplainBuiltin` that will be called from our builtin function
3. Use the optimizer machinery to create and use hypothetical tables/indexes

The method will be similar to:
```go
func (p *planner) HypoIndexExplainBuiltin(
    ctx context.Context, 
    indexes tree.Datum,
    query tree.Datum,
    options tree.Datum,
) (tree.Datum, error) {
    // Implementation similar to makeQueryIndexRecommendation
    // but adapted for our specific use case
}
```

### Step 4: Testing Strategy
Our implementation includes multiple types of tests:

1. **Unit Tests**:
   - Created test file `pkg/sql/sem/builtins/hypo_index_test.go`
   - Added tests for parameter validation
   - Added tests for error cases
   - Need to add tests for the actual optimizer integration

2. **Logic Tests**:
   - Created test file `pkg/sql/logictest/testdata/logic_test/hypo_index_explain`
   - Added tests for basic functionality
   - Added tests for multiple indexes
   - Added tests for error cases
   - Need to update these with actual EXPLAIN output once Phase 3 is complete

3. **Manual Verification**:
   - Created `pkg/sql/hypo_index_explain_manual_test.go` for manual verification

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
