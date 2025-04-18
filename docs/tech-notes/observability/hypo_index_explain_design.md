# Implementation Design for `hypo_index_explain`

## Overview
This document outlines the design and implementation plan for the `hypo_index_explain` function in CockroachDB. This feature simulates the behavior of PostgreSQL's HypoPG extension, allowing users to see the execution plan for a query as if hypothetical indexes existed without actually creating them.

## Background
In PostgreSQL, the HypoPG extension (https://github.com/HypoPG/hypopg) allows users to create hypothetical indexes that the query planner can use to generate query plans without materializing the indexes. This helps database administrators evaluate different indexing strategies without affecting database performance or making actual schema changes.

CockroachDB already has infrastructure for index recommendations and hypothetical indexes in the context of the index recommendation system, which we can leverage for this feature.

## Design Goals
1. Provide a function that accepts a list of hypothetical indexes and a SQL query
2. Generate an EXPLAIN plan that shows how the query would be executed if the hypothetical indexes existed
3. Ensure the function is easy to use and provides meaningful results
4. Leverage existing CockroachDB infrastructure for hypothetical indexes
5. Not generate additional index recommendations in the EXPLAIN output

## Function Interface
```sql
SELECT hypo_index_explain(
  ARRAY[
    'CREATE INDEX hypo_idx1 ON schema.table1 (col1, col2)',
    'CREATE INDEX hypo_idx2 ON schema.table2 (col3) STORING (col4)'
  ],
  'SELECT * FROM schema.table1 JOIN schema.table2 ON table1.id = table2.id WHERE col1 > 10 AND col3 < 100'
);
```

## Implementation Approach

### 1. Function Definition
Create a SQL function named `hypo_index_explain` that accepts:
- An array of strings representing `CREATE INDEX` statements for hypothetical indexes
- A SQL query string to be explained using these hypothetical indexes
- An optional parameter for the EXPLAIN options (e.g., VERBOSE, ANALYZE)

### 2. Parse Hypothetical Indexes
- Parse each `CREATE INDEX` statement into a structured representation
- Validate that each statement is a valid `CREATE INDEX` statement
- Extract table name, column names, and other index properties (e.g., STORING clause, index type)

### 3. Create Hypothetical Indexes
- Create hypothetical index objects based on the parsed `CREATE INDEX` statements
- Use `indexrec.BuildOptAndHypTableMaps` with the custom hypothetical indexes instead of candidates derived from the query

### 4. Build Optimized Plan with Hypothetical Indexes
- Adapt the approach from `makeQueryIndexRecommendation()`:
  - Parse the input SQL query
  - Build an initial memo with the optimizer
  - Create hypothetical tables with the provided hypothetical indexes
  - Optimize the query using these hypothetical tables
  - Ensure that index recommendations are disabled (`p.SessionData().IndexRecommendationsEnabled = false`)

### 5. Generate EXPLAIN Output
- Generate the EXPLAIN plan for the optimized query
- Format the output to clearly indicate which hypothetical indexes are being used

## Detailed Implementation Steps

### Step 1: Create the SQL Function
Create a new built-in function in the SQL package with the following signature:
```go
func HypoIndexExplain(
    ctx context.Context,
    hypotheticalIndexes []string,
    query string,
    explainOptions string,
) (string, error)
```

### Step 2: Parse the Hypothetical Indexes
For each `CREATE INDEX` statement:
1. Parse it using the SQL parser
2. Validate it's a `CREATE INDEX` statement
3. Extract relevant information:
   - Table name
   - Index name
   - Column definitions
   - STORING clause
   - Uniqueness
   - Index type (B-tree, inverted, etc.)

### Step 3: Create a Structure Similar to Index Candidates
1. Adapt the output from the parsing step to a format compatible with the optimizer's hypothetical index infrastructure
2. Convert the parsed indexes into the format expected by `BuildOptAndHypTableMaps`

### Step 4: Modify the Query Optimization Process
1. Based on `makeQueryIndexRecommendation`:
   - Parse and normalize the query
   - Save the normalized memo
   - Create hypothetical tables with our custom indexes
   - Optimize the query with these hypothetical tables
   - Temporarily disable index recommendations during the process

### Step 5: Generate and Format the EXPLAIN Output
1. Use the optimized plan to generate the EXPLAIN output with the specified options
2. Format the output to make it clear which hypothetical indexes are being used in the plan

### Step 6: Register and Document the Function
1. Register the function in the SQL catalog with appropriate type signatures
2. Add comprehensive documentation and examples

## Code Structure
The implementation will be organized as follows:

1. **Function Registration**: Register the `hypo_index_explain` function in `pkg/sql/sem/builtins/`
2. **Core Implementation**: Implement the function in a new file `pkg/sql/hypo_index_explain.go`
3. **Parsing Logic**: Create parsing functions to handle the hypothetical index definitions
4. **Integration with Optimizer**: Adapt the optimization logic from `makeQueryIndexRecommendation`

## Detailed Design for Core Components

### Parsing Hypothetical Indexes
```go
// parseHypotheticalIndexes parses an array of CREATE INDEX statements into a structured format
func parseHypotheticalIndexes(ctx context.Context, indexStmts []string) ([]hypotheticalIndexInfo, error) {
    var result []hypotheticalIndexInfo
    
    for _, stmt := range indexStmts {
        // Parse the statement using the SQL parser
        parsed, err := parser.ParseOne(stmt)
        if err != nil {
            return nil, errors.Wrapf(err, "failed to parse index statement: %s", stmt)
        }
        
        // Ensure it's a CREATE INDEX statement
        createIdx, ok := parsed.AST.(*tree.CreateIndex)
        if !ok {
            return nil, errors.Newf("statement is not a CREATE INDEX statement: %s", stmt)
        }
        
        // Extract information from the statement
        info := hypotheticalIndexInfo{
            TableName:     createIdx.Table.String(),
            IndexName:     string(createIdx.Name),
            Columns:       createIdx.Columns,
            Storing:       createIdx.Storing,
            Unique:        createIdx.Unique,
            IndexType:     createIdx.Type,
            OriginalStmt:  stmt,
        }
        
        result = append(result, info)
    }
    
    return result, nil
}
```

### Converting to Index Candidates
```go
// buildIndexCandidates converts parsed hypothetical indexes into index candidates
// compatible with the optimizer's hypothetical index infrastructure
func buildIndexCandidates(
    ctx context.Context, 
    txn *kv.Txn,
    ie sqlutil.InternalExecutor,
    parsedIndexes []hypotheticalIndexInfo,
) (map[cat.Table][]indexrec.HypotheticalIndex, error) {
    // Logic to convert parsed indexes into the format expected by indexrec
    // Will need to look up table descriptors and convert the parsed information
    // into the format used by indexrec.BuildOptAndHypTableMaps
}
```

### Adapting makeQueryIndexRecommendation
```go
// hypoIndexExplain implements the core logic for the hypo_index_explain function
func hypoIndexExplain(
    ctx context.Context,
    p *planner,
    hypotheticalIndexes []string,
    query string,
    explainOptions string,
) (string, error) {
    // Parse hypothetical indexes
    parsedIndexes, err := parseHypotheticalIndexes(ctx, hypotheticalIndexes)
    if err != nil {
        return "", err
    }
    
    // Parse the query
    stmt, err := parser.ParseOne(query)
    if err != nil {
        return "", errors.Wrapf(err, "failed to parse query: %s", query)
    }
    
    // Create an optimizer context and build the initial memo
    var opc optPlanningCtx
    opc.init(ctx, p)
    
    // Build index candidates from parsed indexes
    indexCandidates, err := buildIndexCandidates(ctx, p.Txn(), p.ExtendedEvalContext().InternalExecutor, parsedIndexes)
    if err != nil {
        return "", err
    }
    
    // Create hypothetical tables with custom indexes
    optTables, hypTables := indexrec.BuildOptAndHypTableMaps(opc.catalog, indexCandidates)
    
    // Save the original setting and temporarily disable index recommendations
    origIndexRecEnabled := p.SessionData().IndexRecommendationsEnabled
    p.SessionData().IndexRecommendationsEnabled = false
    defer func() {
        p.SessionData().IndexRecommendationsEnabled = origIndexRecEnabled
    }()
    
    // Optimize the query with hypothetical tables
    // (Adapting the logic from makeQueryIndexRecommendation)
    
    // Generate EXPLAIN output with the optimized plan
    // ...
    
    return explainOutput, nil
}
```

## Testing Strategy
1. **Unit Tests**:
   - Test parsing of hypothetical index definitions
   - Test conversion to index candidates
   - Test optimization with hypothetical indexes

2. **Integration Tests**:
   - Test with various combinations of hypothetical indexes and queries
   - Test with different table schemas and data distributions
   - Test with different EXPLAIN options

3. **Edge Cases**:
   - Invalid index definitions
   - Invalid queries
   - Indexes that would not be used
   - Very complex queries with many potential index choices

## Limitations and Future Enhancements
1. Initial implementation might not support all index types or options
2. Future enhancements could include:
   - A session-level API for managing hypothetical indexes
   - Cost estimation comparisons between different index configurations
   - Integration with index recommendation system
   - Visual comparison of execution plans with different index combinations

## Conclusion
The proposed design leverages CockroachDB's existing infrastructure for hypothetical indexes and query optimization to implement a feature similar to PostgreSQL's HypoPG extension. By adapting the approach used in `makeQueryIndexRecommendation`, we can create a user-friendly function that helps users evaluate the potential impact of different indexing strategies without requiring actual schema changes. 
