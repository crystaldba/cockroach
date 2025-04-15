// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hypoindex

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// QueryAnalyzer provides functionality to analyze SQL queries and recommend indexes
type QueryAnalyzer struct {
	sqlExecutor SQLExecutor
}

// NewQueryAnalyzer creates a new QueryAnalyzer
func NewQueryAnalyzer(sqlExecutor SQLExecutor) *QueryAnalyzer {
	return &QueryAnalyzer{
		sqlExecutor: sqlExecutor,
	}
}

// IndexRecommendation represents a recommended index
type IndexRecommendation struct {
	TableSchema    string
	TableName      string
	Columns        []string
	Storing        []string
	EstimatedValue float64 // 0-100 scale with 100 being highest value
	Explanation    string
	Unique         bool
	WhereFilter    string // For partial indexes
}

// AnalysisResult contains the results of query analysis
type AnalysisResult struct {
	ParsedQuery        string
	TablesReferenced   []string
	ColumnsFiltered    map[string][]string // Table -> columns used in filters
	ColumnsProjected   map[string][]string // Table -> columns used in result set
	ColumnsJoined      map[string][]string // Table -> columns used in joins
	ColumnsSorted      map[string][]string // Table -> columns used in ORDER BY
	RecommendedIndexes []IndexRecommendation
	QueryType          string // SELECT, INSERT, UPDATE, DELETE
}

// AnalyzeQuery analyzes a SQL query and returns recommendations
func (qa *QueryAnalyzer) AnalyzeQuery(ctx context.Context, query string) (*AnalysisResult, error) {
	// Parse the query
	stmt, err := parser.ParseOne(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	// Create the result object
	result := &AnalysisResult{
		ParsedQuery:      stmt.AST.String(),
		TablesReferenced: make([]string),
		ColumnsFiltered:  make(map[string][]string),
		ColumnsProjected: make(map[string][]string),
		ColumnsJoined:    make(map[string][]string),
		ColumnsSorted:    make(map[string][]string),
	}

	// Set query type
	result.QueryType = getQueryType(stmt.AST)

	// Extract tables and expressions
	tables, err := extractTables(stmt.AST)
	if err != nil {
		return nil, err
	}
	result.TablesReferenced = tables

	// Analyze based on query type
	switch stmt.AST.(type) {
	case *tree.Select:
		qa.analyzeSelectStatement(stmt.AST.(*tree.Select), result)
	case *tree.Update:
		qa.analyzeUpdateStatement(stmt.AST.(*tree.Update), result)
	case *tree.Delete:
		qa.analyzeDeleteStatement(stmt.AST.(*tree.Delete), result)
	}

	// Generate recommendations
	result.RecommendedIndexes = qa.generateRecommendations(result)

	return result, nil
}

// getQueryType returns the type of query as a string
func getQueryType(stmt tree.Statement) string {
	switch stmt.(type) {
	case *tree.Select:
		return "SELECT"
	case *tree.Update:
		return "UPDATE"
	case *tree.Insert:
		return "INSERT"
	case *tree.Delete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// extractTables extracts table names from a statement
func extractTables(stmt tree.Statement) ([]string, error) {
	// This is a simplified version - in practice, a more sophisticated
	// table extraction would be used that handles aliases, CTEs, etc.
	var tables []string

	switch node := stmt.(type) {
	case *tree.Select:
		if node.Select != nil {
			if selectClause, ok := node.Select.(*tree.SelectClause); ok {
				for _, from := range selectClause.From.Tables {
					if aliased, ok := from.(*tree.AliasedTableExpr); ok {
						if tableName, ok := aliased.Expr.(*tree.TableName); ok {
							tables = append(tables, tableName.Table())
						}
					}
				}
			}
		}
	case *tree.Update:
		if node.Table != nil {
			if aliased, ok := node.Table.(*tree.AliasedTableExpr); ok {
				if tableName, ok := aliased.Expr.(*tree.TableName); ok {
					tables = append(tables, tableName.Table())
				}
			}
		}
	case *tree.Delete:
		if node.Table != nil {
			if aliased, ok := node.Table.(*tree.AliasedTableExpr); ok {
				if tableName, ok := aliased.Expr.(*tree.TableName); ok {
					tables = append(tables, tableName.Table())
				}
			}
		}
	}

	return tables, nil
}

// analyzeSelectStatement analyzes a SELECT statement for index recommendations
func (qa *QueryAnalyzer) analyzeSelectStatement(select_ *tree.Select, result *AnalysisResult) {
	if selectClause, ok := select_.Select.(*tree.SelectClause); ok {
		// Analyze WHERE clause
		if selectClause.Where != nil && selectClause.Where.Expr != nil {
			qa.analyzeWhereClause(selectClause.Where.Expr, result)
		}

		// Analyze SELECT list (projections)
		for _, selExpr := range selectClause.Exprs {
			qa.analyzeProjection(selExpr, result)
		}

		// Analyze ORDER BY
		if select_.OrderBy != nil {
			for _, order := range select_.OrderBy {
				qa.analyzeOrderBy(order, result)
			}
		}

		// Analyze GROUP BY
		if selectClause.GroupBy != nil {
			for _, groupExpr := range selectClause.GroupBy {
				qa.analyzeGroupBy(groupExpr, result)
			}
		}
	}
}

// analyzeUpdateStatement analyzes an UPDATE statement for index recommendations
func (qa *QueryAnalyzer) analyzeUpdateStatement(update *tree.Update, result *AnalysisResult) {
	// Analyze WHERE clause
	if update.Where != nil && update.Where.Expr != nil {
		qa.analyzeWhereClause(update.Where.Expr, result)
	}
}

// analyzeDeleteStatement analyzes a DELETE statement for index recommendations
func (qa *QueryAnalyzer) analyzeDeleteStatement(delete *tree.Delete, result *AnalysisResult) {
	// Analyze WHERE clause
	if delete.Where != nil && delete.Where.Expr != nil {
		qa.analyzeWhereClause(delete.Where.Expr, result)
	}
}

// analyzeWhereClause analyzes WHERE expressions to find filter conditions
func (qa *QueryAnalyzer) analyzeWhereClause(expr tree.Expr, result *AnalysisResult) {
	switch expr := expr.(type) {
	case *tree.ComparisonExpr:
		// Extract column references from comparison
		qa.analyzeComparisonExpr(expr, result)

	case *tree.AndExpr:
		// Recursively analyze AND conditions
		qa.analyzeWhereClause(expr.Left, result)
		qa.analyzeWhereClause(expr.Right, result)

	case *tree.OrExpr:
		// Recursively analyze OR conditions
		qa.analyzeWhereClause(expr.Left, result)
		qa.analyzeWhereClause(expr.Right, result)

	case *tree.FuncExpr:
		// Extract column references from function
		qa.analyzeFunctionExpr(expr, result)
	}
}

// analyzeComparisonExpr extracts column references from comparison expressions
func (qa *QueryAnalyzer) analyzeComparisonExpr(expr *tree.ComparisonExpr, result *AnalysisResult) {
	// Look for column names on left side
	if col, table := extractColumnRef(expr.Left); col != "" {
		if _, exists := result.ColumnsFiltered[table]; !exists {
			result.ColumnsFiltered[table] = []string{}
		}
		result.ColumnsFiltered[table] = appendUniqueStr(result.ColumnsFiltered[table], col)
	}

	// Look for column names on right side
	if col, table := extractColumnRef(expr.Right); col != "" {
		if _, exists := result.ColumnsFiltered[table]; !exists {
			result.ColumnsFiltered[table] = []string{}
		}
		result.ColumnsFiltered[table] = appendUniqueStr(result.ColumnsFiltered[table], col)
	}
}

// analyzeFunctionExpr extracts column references from function expressions
func (qa *QueryAnalyzer) analyzeFunctionExpr(expr *tree.FuncExpr, result *AnalysisResult) {
	// Extract columns from function arguments
	for _, arg := range expr.Exprs {
		if col, table := extractColumnRef(arg); col != "" {
			if _, exists := result.ColumnsFiltered[table]; !exists {
				result.ColumnsFiltered[table] = []string{}
			}
			result.ColumnsFiltered[table] = appendUniqueStr(result.ColumnsFiltered[table], col)
		}
	}
}

// analyzeProjection extracts column references from the SELECT list
func (qa *QueryAnalyzer) analyzeProjection(expr tree.SelectExpr, result *AnalysisResult) {
	if starExpr, ok := expr.Expr.(*tree.UnresolvedName); ok {
		// Handle * expressions
		if starExpr.NumParts == 1 && starExpr.Parts[0] == "*" {
			// All columns from all tables, handled elsewhere
			return
		}
	}

	if col, table := extractColumnRef(expr.Expr); col != "" {
		if _, exists := result.ColumnsProjected[table]; !exists {
			result.ColumnsProjected[table] = []string{}
		}
		result.ColumnsProjected[table] = appendUniqueStr(result.ColumnsProjected[table], col)
	}
}

// analyzeOrderBy extracts column references from ORDER BY clauses
func (qa *QueryAnalyzer) analyzeOrderBy(orderBy tree.OrderBy, result *AnalysisResult) {
	for _, order := range orderBy {
		if col, table := extractColumnRef(order.Expr); col != "" {
			if _, exists := result.ColumnsSorted[table]; !exists {
				result.ColumnsSorted[table] = []string{}
			}
			result.ColumnsSorted[table] = appendUniqueStr(result.ColumnsSorted[table], col)
		}
	}
}

// analyzeGroupBy extracts column references from GROUP BY clauses
func (qa *QueryAnalyzer) analyzeGroupBy(expr tree.Expr, result *AnalysisResult) {
	if col, table := extractColumnRef(expr); col != "" {
		if _, exists := result.ColumnsSorted[table]; !exists {
			result.ColumnsSorted[table] = []string{}
		}
		result.ColumnsSorted[table] = appendUniqueStr(result.ColumnsSorted[table], col)
	}
}

// extractColumnRef attempts to extract a column name from an expression
// Returns column name and table name (if available)
func extractColumnRef(expr tree.Expr) (string, string) {
	switch expr := expr.(type) {
	case *tree.UnresolvedName:
		if expr.NumParts == 1 {
			// Just column
			return expr.Parts[0], ""
		} else if expr.NumParts == 2 {
			// Table.column
			return expr.Parts[1], expr.Parts[0]
		}
	}
	return "", ""
}

// generateRecommendations creates index recommendations based on analysis
func (qa *QueryAnalyzer) generateRecommendations(result *AnalysisResult) []IndexRecommendation {
	var recommendations []IndexRecommendation

	// For each table, generate recommendations
	for _, table := range result.TablesReferenced {
		tableFilterCols := result.ColumnsFiltered[table]
		tableSortCols := result.ColumnsSorted[table]
		tableProjectCols := result.ColumnsProjected[table]

		// Skip tables with no filter conditions (usually not worth indexing)
		if len(tableFilterCols) == 0 {
			continue
		}

		// Basic recommendation: index on filtered columns
		if len(tableFilterCols) > 0 {
			// Create an index with filter columns first
			rec := IndexRecommendation{
				TableName:      table,
				TableSchema:    "public", // Assumed for now
				Columns:        make([]string, len(tableFilterCols)),
				EstimatedValue: calculateIndexValue(tableFilterCols, tableSortCols, tableProjectCols),
				Explanation:    fmt.Sprintf("Suggested index for filtering on columns: %s", strings.Join(tableFilterCols, ", ")),
			}
			copy(rec.Columns, tableFilterCols)

			// Add sort columns to the index if they're not already included
			for _, sortCol := range tableSortCols {
				if !contains(rec.Columns, sortCol) {
					rec.Columns = append(rec.Columns, sortCol)
				}
			}

			// Add potential STORING columns for covering index benefits
			for _, projCol := range tableProjectCols {
				if !contains(rec.Columns, projCol) {
					rec.Storing = append(rec.Storing, projCol)
				}
			}

			// Limit to practical sizes
			if len(rec.Columns) > 3 {
				rec.Columns = rec.Columns[:3]
			}
			if len(rec.Storing) > 3 {
				rec.Storing = rec.Storing[:3]
			}

			recommendations = append(recommendations, rec)
		}
	}

	// Sort recommendations by estimated value
	sort.Slice(recommendations, func(i, j int) bool {
		return recommendations[i].EstimatedValue > recommendations[j].EstimatedValue
	})

	return recommendations
}

// calculateIndexValue estimates how beneficial an index would be (0-100 scale)
func calculateIndexValue(filterCols, sortCols, projectCols []string) float64 {
	// This is a simplified model - in practice, a more sophisticated algorithm would be used
	// that takes into account query frequency, data distribution, etc.

	// Base value from filter columns (most important for performance)
	value := float64(len(filterCols)) * 25

	// Additional value from sort columns
	value += float64(len(sortCols)) * 15

	// Covering index bonus (if we can include all projected columns)
	projectionOverlap := 0
	for _, col := range projectCols {
		if contains(filterCols, col) || contains(sortCols, col) {
			projectionOverlap++
		}
	}
	coveringBonus := float64(projectionOverlap) / float64(max(1, len(projectCols))) * 20
	value += coveringBonus

	// Cap at 100
	if value > 100 {
		value = 100
	}

	return value
}

// contains checks if a string slice contains a value
func contains(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

// appendUniqueStr adds a string to a slice if it doesn't already exist
func appendUniqueStr(slice []string, val string) []string {
	if contains(slice, val) {
		return slice
	}
	return append(slice, val)
}

// max returns the larger of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// FormatRecommendations formats the recommendations as a human-readable string
func FormatRecommendations(recommendations []IndexRecommendation) string {
	var sb strings.Builder

	sb.WriteString("Recommended Indexes:\n")

	if len(recommendations) == 0 {
		sb.WriteString("  No specific index recommendations identified for this query.\n")
		return sb.String()
	}

	for i, rec := range recommendations {
		sb.WriteString(fmt.Sprintf("%d. Table: %s.%s\n", i+1, rec.TableSchema, rec.TableName))
		sb.WriteString(fmt.Sprintf("   Columns: %s\n", strings.Join(rec.Columns, ", ")))

		if len(rec.Storing) > 0 {
			sb.WriteString(fmt.Sprintf("   STORING: %s\n", strings.Join(rec.Storing, ", ")))
		}

		if rec.Unique {
			sb.WriteString("   Type: UNIQUE\n")
		}

		if rec.WhereFilter != "" {
			sb.WriteString(fmt.Sprintf("   Partial WHERE: %s\n", rec.WhereFilter))
		}

		sb.WriteString(fmt.Sprintf("   Value: %.1f%%\n", rec.EstimatedValue))
		sb.WriteString(fmt.Sprintf("   Explanation: %s\n", rec.Explanation))
	}

	return sb.String()
}

// CreateIndexSQL generates SQL statements to create the recommended indexes
func CreateIndexSQL(recommendations []IndexRecommendation) []string {
	var statements []string

	for i, rec := range recommendations {
		indexName := fmt.Sprintf("idx_%s_%d", rec.TableName, i+1)

		sql := fmt.Sprintf("CREATE %sINDEX %s ON %s.%s (%s)",
			ternary(rec.Unique, "UNIQUE ", ""),
			indexName,
			rec.TableSchema,
			rec.TableName,
			strings.Join(rec.Columns, ", "))

		if len(rec.Storing) > 0 {
			sql += fmt.Sprintf(" STORING (%s)", strings.Join(rec.Storing, ", "))
		}

		if rec.WhereFilter != "" {
			sql += fmt.Sprintf(" WHERE %s", rec.WhereFilter)
		}

		sql += ";"
		statements = append(statements, sql)
	}

	return statements
}

// CreateHypotheticalIndexSQL generates SQL to create hypothetical indexes
func CreateHypotheticalIndexSQL(recommendations []IndexRecommendation) []string {
	var statements []string

	for i, rec := range recommendations {
		indexName := fmt.Sprintf("hypo_%s_%d", rec.TableName, i+1)

		// Convert the columns array to a format suitable for the function call
		columnsArray := fmt.Sprintf("ARRAY[%s]",
			strings.Join(quoteStringArray(rec.Columns), ", "))

		var storingArray string
		if len(rec.Storing) > 0 {
			storingArray = fmt.Sprintf("ARRAY[%s]",
				strings.Join(quoteStringArray(rec.Storing), ", "))
		} else {
			storingArray = "NULL"
		}

		sql := fmt.Sprintf("SELECT pg_extension.hypo_create_index('%s', '%s', '%s', %s, %s, %t, false, '%s');",
			rec.TableSchema,
			rec.TableName,
			indexName,
			columnsArray,
			storingArray,
			rec.Unique,
			fmt.Sprintf("Recommended index #%d: %s", i+1, rec.Explanation))

		statements = append(statements, sql)
	}

	return statements
}

// quoteStringArray returns a slice of quoted strings
func quoteStringArray(arr []string) []string {
	quoted := make([]string, len(arr))
	for i, s := range arr {
		quoted[i] = fmt.Sprintf("'%s'", s)
	}
	return quoted
}

// ternary is a simple ternary operator implementation for strings
func ternary(condition bool, trueVal, falseVal string) string {
	if condition {
		return trueVal
	}
	return falseVal
}
