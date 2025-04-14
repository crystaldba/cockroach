# Complete Implementation Guide for hypoindex

This document outlines the steps to fully implement the `hypoindex` extension with CockroachDB's query optimizer.

## High-Level Architecture

To fully integrate with CockroachDB's internal optimizer, the extension needs to:

1. Create and manage hypothetical index definitions in a system table
2. Wrap actual catalog tables with hypothetical tables that include the hypothetical indexes
3. Run the optimizer with these hypothetical tables
4. Generate EXPLAIN output based on the optimized plans

## Core Components

### 1. Integration with indexrec.HypotheticalTable

The key component needed is integration with the `indexrec.HypotheticalTable` type, which already provides the ability to create hypothetical indexes on tables.

```go
// Create a HypotheticalTable for each table with hypothetical indexes
optTables, hypTables := indexrec.BuildOptAndHypTableMaps(catalog, indexCandidates)

// Use the optimizer with these tables
memo.Metadata().UpdateTableMeta(ctx, evalCtx, hypTables)
_, err = optimizer.Optimize()
```

### 2. Connection to the Query Optimizer

The extension needs access to these internal components:

- `opt.Optimizer` - For running optimization with hypothetical tables
- `memo.Memo` - For accessing the memo after optimization
- `cat.Catalog` - For finding tables and building hypothetical versions

### 3. Conversion between SQL and Internal Representations

The extension needs to:

- Convert user-defined index definitions to `cat.IndexColumn` arrays
- Build the correct `indexCandidates` map structure
- Extract explain output from the optimized plan

## Complete Implementation Steps

1. **Extension Registration**
   - Register with CockroachDB's extension system
   - Create system tables and functions

2. **SQL Function Implementations**
   - Implement `hypo_create_index` to store index definitions
   - Implement `hypo_explain` to run optimizer with hypothetical indexes

3. **Optimizer Integration**
   - Create `indexrec.HypotheticalTable` objects
   - Build index candidates from stored definitions
   - Run optimizer with hypothetical tables

4. **Explain Implementation**
   - Use optimizer to get the plan with hypothetical indexes
   - Convert to EXPLAIN format
   - Return to the user

## Example of Full Implementation

```go
func (h *HypoIndexExplainer) ExplainQuery(ctx context.Context, query string) (string, error) {
    // 1. Parse the query
    stmt, err := parser.ParseOne(query)
    if err != nil {
        return "", err
    }

    // 2. Get hypothetical indexes from the system table
    hypoIndexes, err := h.fetchHypotheticalIndexes(ctx)
    if err != nil {
        return "", err
    }

    // 3. Find all tables referenced in the query
    tables, err := h.findReferencedTables(ctx, stmt.AST)
    if err != nil {
        return "", err
    }

    // 4. Build index candidates for each table
    indexCandidates := h.buildIndexCandidates(tables, hypoIndexes)

    // 5. Create hypothetical tables
    optTables, hypTables := indexrec.BuildOptAndHypTableMaps(h.catalog, indexCandidates)

    // 6. Save current memo and create optimizer
    optimizer := memo.New()
    optimizer.Init(ctx, h.evalCtx, h.catalog)

    // 7. Update metadata with hypothetical tables
    optimizer.Memo().Metadata().UpdateTableMeta(ctx, h.evalCtx, hypTables)

    // 8. Optimize the query
    optimizedExpr, err := optimizer.Optimize()
    if err != nil {
        return "", err
    }

    // 9. Generate explain output
    explainStr := h.generateExplainOutput(optimizedExpr)

    // 10. Restore original tables
    optimizer.Memo().Metadata().UpdateTableMeta(ctx, h.evalCtx, optTables)

    return explainStr, nil
}
```

## Technical Challenges

1. **Access to Internal APIs**
   - Need access to internal optimizer components
   - May require exposing additional APIs for extensions

2. **Stability and Compatibility**
   - Internal APIs can change between versions
   - Need to maintain compatibility

3. **Performance Considerations**
   - Creating hypothetical tables has overhead
   - Should be optimized for large workloads

## Recommended Development Approach

1. Start with a standalone extension package
2. Gradually integrate with internal APIs
3. Write comprehensive tests
4. Validate results against actual index creation

## References

- `pkg/sql/opt/indexrec/hypothetical_table.go` - Implementation of HypotheticalTable
- `pkg/sql/opt/testutils/opttester/opt_tester.go` - OptimizeWithTables function
- `pkg/sql/opt/memo/memo.go` - Memo implementation with UpdateTableMeta 
