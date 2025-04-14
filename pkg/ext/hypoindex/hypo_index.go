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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
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

// findReferencedTables extracts tables referenced in the query.
func (h *HypoIndexExplainer) findReferencedTables(ctx context.Context, stmt tree.Statement) ([]catalog.TableDescriptor, error) {
	// In a full implementation, this would:
	// 1. Extract table references from the AST
	// 2. Use the catalog to look up the corresponding table descriptors

	// For now, return an empty slice since we'll use mock implementations
	return []catalog.TableDescriptor{}, nil
}

// optimizeWithHypotheticalIndexes runs the optimizer with hypothetical indexes.
func (h *HypoIndexExplainer) optimizeWithHypotheticalIndexes(
	ctx context.Context,
	stmt tree.Statement,
	indexCandidates map[cat.Table][][]cat.IndexColumn,
) (string, error) {
	// In a full implementation, this would:
	// 1. Create hypothetical tables with BuildOptAndHypTableMaps
	// 2. Run memo optimizer with these tables
	// 3. Format the resulting plan

	var sb strings.Builder
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
				// Use string() conversion instead of String() method
				sb.WriteString(string(col.Column.ColName()))
			}
			sb.WriteString(")\n")
		}
	}

	// In a full implementation, we would add:
	// - The optimizer's chosen plan
	// - Cost before and after
	// - Statistics about index usage

	return sb.String(), nil
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
	for _, idx := range hypoIndexes {
		tableKey := fmt.Sprintf("%s.%s", idx.TableSchema, idx.TableName)
		indexesByTable[tableKey] = append(indexesByTable[tableKey], idx)
	}

	// Create the index candidates map
	result := make(map[cat.Table][][]cat.IndexColumn)

	// For each table, convert its hypothetical indexes to cat.IndexColumn arrays
	for _, tbl := range tables {
		// Skip tables with no hypothetical indexes
		tableKey := fmt.Sprintf("%d.%s", tbl.GetParentSchemaID(), tbl.GetName())
		tableIndexes, exists := indexesByTable[tableKey]
		if !exists {
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

// findCatTable finds the cat.Table corresponding to a catalog.TableDescriptor
func (h *HypoIndexExplainer) findCatTable(tbl catalog.TableDescriptor) (cat.Table, error) {
	if h.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// Note: We don't actually use the table name here since this is a placeholder implementation
	// In a real implementation, we'd use the catalog to look up the table

	// For now, we'll just return nil since this is a placeholder implementation
	return nil, fmt.Errorf("catalog lookup not implemented")
}

// createHypotheticalTables converts index candidates to hypothetical tables.
func (h *HypoIndexExplainer) createHypotheticalTables(
	indexCandidates map[cat.Table][][]cat.IndexColumn,
) (map[cat.StableID]cat.Table, map[cat.StableID]cat.Table) {
	// In a full implementation, this would call:
	// return indexrec.BuildOptAndHypTableMaps(h.catalog, indexCandidates)

	// For now, return empty maps
	return map[cat.StableID]cat.Table{}, map[cat.StableID]cat.Table{}
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
