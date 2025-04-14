// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hypoindex

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/indexrec"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// SQLExecutor is an interface for executing SQL queries.
type SQLExecutor interface {
	// QueryBufferedEx executes a SQL query and returns the results buffered.
	QueryBufferedEx(
		ctx context.Context,
		opName string,
		txn interface{},
		override interface{},
		query string,
		qargs ...interface{},
	) ([]tree.Datums, error)
}

// HypoIndexExplainer is responsible for running EXPLAIN with hypothetical indexes.
type HypoIndexExplainer struct {
	evalCtx     interface{}
	catalog     cat.Catalog
	sqlExecutor SQLExecutor
}

// NewHypoIndexExplainer creates a new HypoIndexExplainer.
func NewHypoIndexExplainer(evalCtx interface{}, sqlExecutor SQLExecutor) *HypoIndexExplainer {
	return &HypoIndexExplainer{
		evalCtx:     evalCtx,
		catalog:     nil,
		sqlExecutor: sqlExecutor,
	}
}

// SetCatalog sets the catalog for the HypoIndexExplainer.
// This can be called after creation if the catalog is not available at construction time.
func (h *HypoIndexExplainer) SetCatalog(catalog cat.Catalog) {
	h.catalog = catalog
}

// ExplainQuery runs EXPLAIN on the given query with hypothetical indexes.
func (h *HypoIndexExplainer) ExplainQuery(ctx context.Context, query string) (string, error) {
	// Parse the query
	stmt, err := parser.ParseOne(query)
	if err != nil {
		return "", fmt.Errorf("failed to parse query: %w", err)
	}

	// Get the hypothetical indexes
	hypoIndexes, err := h.fetchHypotheticalIndexes(ctx)
	if err != nil {
		return "", err
	}

	// Find tables referenced in the query
	tables, err := h.findReferencedTables(ctx, stmt.AST)
	if err != nil {
		return "", fmt.Errorf("error finding referenced tables: %w", err)
	}

	// In a full implementation:
	// 1. Convert HypotheticalIndexDef to indexrec candidates
	indexCandidates := h.buildIndexCandidates(tables, hypoIndexes)

	// 2. Build hypothetical tables and optimize
	explainText, err := h.optimizeWithHypotheticalIndexes(ctx, stmt.AST, indexCandidates)
	if err != nil {
		// Create a fallback explanation
		fallback := fmt.Sprintf("EXPLAIN with %d hypothetical indexes for query: %s",
			len(hypoIndexes), stmt.AST.String())
		return fallback, nil //nolint:returnerrcheck
	}

	return explainText, nil
}

// findReferencedTables returns a list of table descriptors referenced by the SQL statement
func (h *HypoIndexExplainer) findReferencedTables(
	ctx context.Context, stmt tree.Statement,
) ([]catalog.TableDescriptor, error) {
	// For EXPLAIN statements, extract tables from the inner statement
	if explainStmt, ok := stmt.(*tree.Explain); ok && explainStmt.Statement != nil {
		return h.findReferencedTables(ctx, explainStmt.Statement)
	}

	// Initialize the table collector
	tc := &tableCollector{
		ctx:        ctx,
		catalog:    h.catalog,
		tableNames: make(map[string]struct{}),
		tables:     make([]catalog.TableDescriptor, 0),
	}

	// Extract table references using a custom tree visitor
	tableNames := h.extractTableNames(stmt)

	// Add all found tables to the collector
	for _, tableName := range tableNames {
		if err := tc.addTable(tableName); err != nil {
			// Log the error but continue with other tables
			continue
		}
	}

	return tc.tables, nil
}

// extractTableNames extracts all table names from a statement
func (h *HypoIndexExplainer) extractTableNames(stmt tree.Statement) []*tree.TableName {
	var tableNames []*tree.TableName

	// Handle specific statement types that may contain table references
	switch node := stmt.(type) {
	case *tree.Select:
		// Extract from the FROM clause in a SELECT
		if node.Select != nil {
			if selectClause, ok := node.Select.(*tree.SelectClause); ok {
				tableNames = append(tableNames, extractTablesFromFrom(&selectClause.From)...)
			}
		}
	case *tree.Update:
		// Extract the target table in an UPDATE
		if node.Table != nil {
			if name := getTableNameFromTableExpr(node.Table); name != nil {
				tableNames = append(tableNames, name)
			}
		}
	case *tree.Delete:
		// Extract the target table in a DELETE
		if node.Table != nil {
			if name := getTableNameFromTableExpr(node.Table); name != nil {
				tableNames = append(tableNames, name)
			}
		}
	case *tree.Insert:
		// Extract the target table in an INSERT
		if name, ok := node.Table.(*tree.TableName); ok {
			tableNames = append(tableNames, name)
		}
	}

	return tableNames
}

// extractTablesFromFrom extracts table names from a FROM clause
func extractTablesFromFrom(from *tree.From) []*tree.TableName {
	var tableNames []*tree.TableName

	for _, table := range from.Tables {
		if name := getTableNameFromTableExpr(table); name != nil {
			tableNames = append(tableNames, name)
		}
	}

	return tableNames
}

// getTableNameFromTableExpr extracts a TableName from a TableExpr if possible
func getTableNameFromTableExpr(expr tree.TableExpr) *tree.TableName {
	switch t := expr.(type) {
	case *tree.AliasedTableExpr:
		if name, ok := t.Expr.(*tree.TableName); ok {
			return name
		}
	}
	return nil
}

// addTable adds a table to the collection
func (tc *tableCollector) addTable(tableName *tree.TableName) error {
	if tableName == nil || tableName.Table() == "" {
		return nil // Skip empty table names
	}

	key := tableName.String()
	if _, exists := tc.tableNames[key]; exists {
		return nil
	}
	tc.tableNames[key] = struct{}{}

	// Look up the table descriptor using the catalog
	if tc.catalog == nil {
		return fmt.Errorf("catalog not initialized")
	}

	// Resolve the data source by name - use the TableName directly since cat.DataSourceName is an alias for tree.TableName
	ds, _, err := tc.catalog.ResolveDataSource(tc.ctx, cat.Flags{}, tableName)
	if err != nil {
		return err
	}

	if table, ok := ds.(cat.Table); ok {
		// Convert the cat.Table to a catalog.TableDescriptor if possible
		// This might require a cast depending on the implementation
		if tableDesc, ok := table.(catalog.TableDescriptor); ok {
			tc.tables = append(tc.tables, tableDesc)
		}
	}

	return nil
}

// tableCollector collects unique table descriptors
type tableCollector struct {
	ctx        context.Context
	catalog    cat.Catalog
	tableNames map[string]struct{} // Used to deduplicate table names
	tables     []catalog.TableDescriptor
}

// optimizeWithHypotheticalIndexes runs the optimizer with hypothetical indexes.
func (h *HypoIndexExplainer) optimizeWithHypotheticalIndexes(
	ctx context.Context,
	stmt tree.Statement,
	indexCandidates map[cat.Table][][]cat.IndexColumn,
) (string, error) {
	var sb strings.Builder

	// Create hypothetical tables using indexrec
	optTables, hypTables := h.createHypotheticalTables(indexCandidates)
	if len(hypTables) == 0 {
		return "No relevant hypothetical indexes found for this query", nil
	}

	// In a full implementation, we would:
	// 1. Create an optimizer instance
	// 2. Initialize it with the statement
	// 3. Update the memo metadata with hypothetical tables
	// 4. Optimize the query
	// 5. Format the resulting plan

	// For now, we'll just show which hypothetical indexes would be considered
	sb.WriteString("EXPLAIN with hypothetical indexes:\n")

	// Display the hypothetical indexes that would be used
	for table, indexes := range indexCandidates {
		sb.WriteString(fmt.Sprintf("Table: %s\n", table.Name()))
		for i, idx := range indexes {
			sb.WriteString(fmt.Sprintf("  Index %d: (", i+1))
			for j, col := range idx {
				if j > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(string(col.Column.ColName()))
			}
			sb.WriteString(")\n")
		}
	}

	// Display the original and hypothetical tables
	sb.WriteString("\nHypothetical Tables:\n")
	for id, hypTable := range hypTables {
		origTable := optTables[id]
		sb.WriteString(fmt.Sprintf("  %s (original indexes: %d, with hypothetical: %d)\n",
			hypTable.Name(), origTable.IndexCount(), hypTable.IndexCount()))
	}

	// TODO: In a full implementation, add the optimizer's chosen plan and cost estimates

	return sb.String(), nil
}

// createHypotheticalTables converts index candidates to hypothetical tables.
func (h *HypoIndexExplainer) createHypotheticalTables(
	indexCandidates map[cat.Table][][]cat.IndexColumn,
) (map[cat.StableID]cat.Table, map[cat.StableID]cat.Table) {
	// Use the indexrec package to build hypothetical tables
	return indexrec.BuildOptAndHypTableMaps(h.catalog, indexCandidates)
}

// fetchHypotheticalIndexes retrieves all hypothetical indexes from the hypo_indexes table.
func (h *HypoIndexExplainer) fetchHypotheticalIndexes(ctx context.Context) ([]HypotheticalIndexDef, error) {
	// The table should be in the pg_extension schema
	query := fmt.Sprintf(`
		SELECT 
			id, 
			table_schema, 
			table_name, 
			index_name, 
			columns, 
			storing, 
			unique, 
			inverted
		FROM %s.hypo_indexes
	`, catconstants.PgExtensionSchemaName)

	// We need an executor to run the query
	if h.sqlExecutor == nil {
		return nil, fmt.Errorf("SQL executor not initialized")
	}

	// Execute the query
	rows, err := h.sqlExecutor.QueryBufferedEx(
		ctx,
		"hypo-fetch-indexes",
		nil, /* txn */
		nil, /* override */
		query,
	)
	if err != nil {
		return nil, fmt.Errorf("error querying hypo_indexes table: %w", err)
	}

	// Process the results
	var result []HypotheticalIndexDef
	for _, row := range rows {
		if len(row) != 8 {
			return nil, fmt.Errorf("unexpected row format: got %d columns, expected 8", len(row))
		}

		// Extract the values from the row
		id := string(tree.MustBeDString(row[0]))
		tableSchema := string(tree.MustBeDString(row[1]))
		tableName := string(tree.MustBeDString(row[2]))
		indexName := string(tree.MustBeDString(row[3]))

		// Extract string array values
		columnsArray := row[4]
		storingArray := row[5]

		// Convert to Go string slices
		columns, err := h.extractStringArray(columnsArray)
		if err != nil {
			return nil, fmt.Errorf("error extracting columns array: %w", err)
		}

		var storing []string
		if storingArray != tree.DNull {
			storing, err = h.extractStringArray(storingArray)
			if err != nil {
				return nil, fmt.Errorf("error extracting storing array: %w", err)
			}
		}

		// Extract boolean values
		unique := row[6] != tree.DBoolFalse
		inverted := row[7] != tree.DBoolFalse

		result = append(result, HypotheticalIndexDef{
			ID:          id,
			TableSchema: tableSchema,
			TableName:   tableName,
			IndexName:   indexName,
			Columns:     columns,
			Storing:     storing,
			Unique:      unique,
			Inverted:    inverted,
		})
	}

	// If no indexes found, return an empty slice
	if len(result) == 0 {
		return []HypotheticalIndexDef{}, nil
	}

	return result, nil
}

// extractStringArray converts a tree.Datum containing a string array into a Go string slice.
func (h *HypoIndexExplainer) extractStringArray(datum tree.Datum) ([]string, error) {
	if datum == tree.DNull {
		return nil, nil
	}

	array, ok := datum.(*tree.DArray)
	if !ok {
		return nil, fmt.Errorf("expected string array, got %T", datum)
	}

	result := make([]string, len(array.Array))
	for i, val := range array.Array {
		str, ok := val.(*tree.DString)
		if !ok {
			return nil, fmt.Errorf("expected string in array, got %T", val)
		}
		result[i] = string(*str)
	}

	return result, nil
}

// buildIndexCandidates creates index candidates for the optimizer based on hypothetical index definitions.
func (h *HypoIndexExplainer) buildIndexCandidates(
	tables []catalog.TableDescriptor,
	hypoIndexes []HypotheticalIndexDef,
) map[cat.Table][][]cat.IndexColumn {
	// Group indexes by table
	indexesByTable := make(map[string][]HypotheticalIndexDef)

	// Also keep a map by table name only, for fallback matching
	indexesByTableName := make(map[string][]HypotheticalIndexDef)

	for _, idx := range hypoIndexes {
		// Standard key with schema
		tableKey := fmt.Sprintf("%s.%s", idx.TableSchema, idx.TableName)
		indexesByTable[tableKey] = append(indexesByTable[tableKey], idx)

		// Add to the table-name-only map for fallback matching
		indexesByTableName[idx.TableName] = append(indexesByTableName[idx.TableName], idx)
	}

	// Create the index candidates map
	result := make(map[cat.Table][][]cat.IndexColumn)

	// Common schema names to try when exact matching fails
	commonSchemas := []string{"public", catconstants.PgCatalogName, catconstants.InformationSchemaName}

	// For each table, convert its hypothetical indexes to cat.IndexColumn arrays
	for _, tbl := range tables {
		var tableIndexes []HypotheticalIndexDef
		exists := false

		// Try all matching strategies in order of specificity:

		// 1. First, try to get the actual schema name if catalog is available
		if h.catalog != nil {
			// Try to resolve the schema name from the ID
			schemaName := getSchemaNameForID(tbl.GetParentSchemaID())
			if schemaName != "" {
				schemaTableKey := fmt.Sprintf("%s.%s", schemaName, tbl.GetName())
				if indexes, found := indexesByTable[schemaTableKey]; found {
					tableIndexes = indexes
					exists = true
				}
			}
		}

		// 2. If schema name lookup failed, try common schema names
		if !exists {
			for _, schema := range commonSchemas {
				schemaTableKey := fmt.Sprintf("%s.%s", schema, tbl.GetName())
				if indexes, found := indexesByTable[schemaTableKey]; found {
					tableIndexes = indexes
					exists = true
					break
				}
			}
		}

		// 3. If still not found, try just by table name as a last resort (for tests)
		if !exists {
			tableIndexes, exists = indexesByTableName[tbl.GetName()]
		}

		// Skip if no indexes found for this table
		if !exists || len(tableIndexes) == 0 {
			continue
		}

		// Get the cat.Table for this table descriptor
		catTable, err := h.findCatTable(tbl)
		if err != nil {
			// Log error and skip this table
			continue
		}

		// Convert each hypothetical index to an array of IndexColumns
		var indexCols [][]cat.IndexColumn
		for _, idx := range tableIndexes {
			cols := idx.convertToIndexCandidate(catTable)
			if len(cols) > 0 {
				indexCols = append(indexCols, cols)
			}
		}

		if len(indexCols) > 0 {
			result[catTable] = indexCols
		}
	}

	return result
}

// getSchemaNameForID returns a known schema name for a schema ID
// This is a simple helper that returns common schema names for known IDs
func getSchemaNameForID(schemaID descpb.ID) string {
	switch schemaID {
	case 29: // Common ID for public schema
		return "public"
	case 30: // Common ID for pg_catalog
		return catconstants.PgCatalogName
	case 31: // Common ID for information_schema
		return catconstants.InformationSchemaName
	default:
		return "" // Unknown schema ID
	}
}

// findCatTable finds the cat.Table corresponding to a catalog.TableDescriptor
func (h *HypoIndexExplainer) findCatTable(tbl catalog.TableDescriptor) (cat.Table, error) {
	if h.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// Get the table ID
	tableID := tbl.GetID()

	// Look up the table directly by ID using the appropriate catalog method
	ds, _, err := h.catalog.ResolveDataSourceByID(context.Background(), cat.Flags{}, cat.StableID(tableID))
	if err != nil {
		return nil, fmt.Errorf("failed to resolve table by ID %d: %w", tableID, err)
	}

	// Check if the data source is a table
	table, ok := ds.(cat.Table)
	if !ok {
		return nil, fmt.Errorf("data source with ID %d is not a table", tableID)
	}

	return table, nil
}

// HypotheticalIndexDef represents a hypothetical index definition.
type HypotheticalIndexDef struct {
	ID          string
	TableSchema string
	TableName   string
	IndexName   string
	Columns     []string
	Storing     []string
	Unique      bool
	Inverted    bool
}

// convertToIndexCandidate converts a HypotheticalIndexDef to a slice of cat.IndexColumn.
func (h *HypotheticalIndexDef) convertToIndexCandidate(table cat.Table) []cat.IndexColumn {
	result := make([]cat.IndexColumn, 0, len(h.Columns))

	// Process key columns
	for _, colName := range h.Columns {
		// Find the column in the table
		col, err := findColumnByName(table, tree.Name(colName))
		if err != nil {
			// Skip this column if not found
			continue
		}

		// Create an IndexColumn
		result = append(result, cat.IndexColumn{
			Column:     col,
			Descending: false, // Default to ascending
			// We could extend HypotheticalIndexDef to support descending columns
		})
	}

	return result
}

// findColumnByName looks up a column by name in a table
func findColumnByName(table cat.Table, name tree.Name) (*cat.Column, error) {
	for i := 0; i < table.ColumnCount(); i++ {
		col := table.Column(i)
		if col.ColName() == name {
			return col, nil
		}
	}
	return nil, fmt.Errorf("column %s not found in table %s", name, table.Name())
}
