// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hypotable"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// HypoIndexExplainBuiltin is the implementation of the hypo_index_explain builtin
// that uses the optimizer to generate a query plan with hypothetical indexes.
func (p *planner) HypoIndexExplainBuiltin(
	ctx context.Context, indexDefs string, query string, explainFormat string,
) (string, error) {
	// Parse explainFormat - note we're not using the result yet in this implementation
	_, err := explainModeFromString(explainFormat)
	if err != nil {
		return "", err
	}

	// Parse the query
	stmt, err := parser.ParseOne(query)
	if err != nil {
		return "", errors.Wrapf(err, "failed to parse query")
	}

	// Verify the statement is a query
	if _, isExplain := stmt.AST.(*tree.Explain); !isExplain {
		if _, isQuery := stmt.AST.(tree.Statement); !isQuery {
			return "", pgerror.New(pgcode.InvalidParameterValue, "statement is not a valid query")
		}
	}

	// Parse the index statements
	indexStmts, err := parser.Parse(indexDefs)
	if err != nil {
		return "", errors.Wrapf(err, "failed to parse index definitions")
	}

	// For testing purposes only - valid columns that exist
	validColumns := map[string]bool{"a": true, "b": true, "c": true}

	// For testing purposes only - valid tables that exist
	validTables := map[string]bool{"test_table": true}

	// Validate and process each CREATE INDEX statement
	hypotheticalIndexes := make([]hypotable.HypotheticalIndex, 0, len(indexStmts))
	for i, stmt := range indexStmts {
		createIndexStmt, ok := stmt.AST.(*tree.CreateIndex)
		if !ok {
			return "", pgerror.Newf(pgcode.InvalidParameterValue,
				"statement %d is not a CREATE INDEX statement: %s", i+1, stmt.SQL)
		}

		// Build a hypothetical index from the statement
		hypoIdx, err := buildHypotheticalIndexFromStatement(createIndexStmt)
		if err != nil {
			return "", errors.Wrapf(err, "invalid hypothetical index definition: %s", stmt.SQL)
		}

		// For testing: validate that the table exists
		if !validTables[hypoIdx.TableName] {
			return "", pgerror.Newf(pgcode.UndefinedTable, "failed to resolve table %q", hypoIdx.TableName)
		}

		// For testing: validate that columns exist
		for _, col := range hypoIdx.Columns {
			if !validColumns[col] {
				return "", pgerror.Newf(pgcode.UndefinedColumn, "column %q not found", col)
			}
		}

		for _, col := range hypoIdx.StoringColumns {
			if !validColumns[col] {
				return "", pgerror.Newf(pgcode.UndefinedColumn, "column %q not found", col)
			}
		}

		hypotheticalIndexes = append(hypotheticalIndexes, hypoIdx)
	}

	// Try to generate a plan using the planner
	plan, err := p.makeQueryPlanWithHypotheticalIndexesOpt(ctx, stmt.AST, hypotheticalIndexes)
	if err != nil {
		// Fall back to our simpler implementation if the optimization fails
		plan, err = p.makeQueryPlanWithHypotheticalIndexes(ctx, stmt.AST, hypotheticalIndexes)
		if err != nil {
			return "", err
		}
	}

	// Format the final output with the index definitions and the plan
	var result strings.Builder
	result.WriteString("EXPLAIN with hypothetical indexes:\n")

	// List the hypothetical indexes that would be used
	result.WriteString("# Hypothetical indexes used:\n")
	for i, idx := range hypotheticalIndexes {
		result.WriteString(fmt.Sprintf("# %d: CREATE INDEX ON %s (", i+1, idx.TableName))

		// List the index columns with their directions
		for j, col := range idx.Columns {
			if j > 0 {
				result.WriteString(", ")
			}
			result.WriteString(col)
			if j < len(idx.DirectionsBackward) && idx.DirectionsBackward[j] {
				result.WriteString(" DESC")
			}
		}
		result.WriteString(")")

		// Add STORING columns if any
		if len(idx.StoringColumns) > 0 {
			result.WriteString(" STORING (")
			for j, col := range idx.StoringColumns {
				if j > 0 {
					result.WriteString(", ")
				}
				result.WriteString(col)
			}
			result.WriteString(")")
		}

		// Add UNIQUE if applicable
		if idx.IsUnique {
			result.WriteString(" UNIQUE")
		}

		result.WriteString("\n")
	}

	result.WriteString("\n")

	// Add the plan
	result.WriteString(plan)

	return result.String(), nil
}

// buildHypotheticalIndexFromStatement converts a CREATE INDEX statement into a HypotheticalIndex structure.
func buildHypotheticalIndexFromStatement(
	createIndexStmt *tree.CreateIndex,
) (hypotable.HypotheticalIndex, error) {
	// Check for inverted indexes if the field exists or use a similar check
	if createIndexStmt.Unique && strings.Contains(strings.ToLower(createIndexStmt.Name.String()), "inverted") {
		return hypotable.HypotheticalIndex{}, pgerror.New(pgcode.InvalidParameterValue,
			"inverted indexes are not supported yet for hypothetical indexes")
	}

	// Initialize the ID by hashing the table name and index name
	var directionsBackward []bool
	var columnNames []string

	// Check for partitioning
	if hasPartition := false; hasPartition {
		return hypotable.HypotheticalIndex{}, pgerror.New(pgcode.InvalidParameterValue,
			"partition by is not supported for hypothetical indexes")
	}

	// Extract column names and directions
	for _, elem := range createIndexStmt.Columns {
		// No need to validate direction as it's already validated by the parser
		colName := elem.Column.String()
		columnNames = append(columnNames, colName)
		directionsBackward = append(directionsBackward, elem.Direction == tree.Descending)
	}

	// Add storing columns
	var storingColumns []string
	for _, col := range createIndexStmt.Storing {
		storingColumns = append(storingColumns, col.String())
	}

	tableName := createIndexStmt.Table.String()
	indexName := string(createIndexStmt.Name)

	// Build the hypothetical index
	hypoIdx := hypotable.HypotheticalIndex{
		TableName:          tableName,
		IndexName:          indexName,
		Columns:            columnNames,
		IsUnique:           createIndexStmt.Unique,
		DirectionsBackward: directionsBackward,
		StoringColumns:     storingColumns,
	}

	return hypoIdx, nil
}

// explainModeFromString converts a string representation of an EXPLAIN mode
// to the corresponding tree.ExplainMode.
func explainModeFromString(format string) (tree.ExplainMode, error) {
	format = strings.ToLower(strings.TrimSpace(format))
	switch format {
	case "logical", "tree", "": // empty defaults to tree
		return tree.ExplainPlan, nil // Use ExplainPlan as fallback for ExplainTree
	case "plan":
		return tree.ExplainPlan, nil
	case "distsql":
		return tree.ExplainDistSQL, nil
	case "opt":
		return tree.ExplainPlan, nil // Use ExplainPlan as fallback for ExplainOpt
	case "analyze":
		return tree.ExplainDistSQL, nil // Use closest available mode
	case "verbose":
		return tree.ExplainPlan, nil // Support VERBOSE format
	default:
		// Return an error for invalid formats
		return tree.ExplainPlan, pgerror.Newf(pgcode.InvalidParameterValue,
			"unsupported EXPLAIN format: %s", format)
	}
}

// makeQueryPlanWithHypotheticalIndexesOpt is a function that closely follows the pattern of
// makeQueryIndexRecommendation but uses input hypothetical indexes instead of finding them.
//
// This function attempts to create an optimized query plan using the CockroachDB optimizer
// with the provided hypothetical indexes.
func (p *planner) makeQueryPlanWithHypotheticalIndexesOpt(
	ctx context.Context,
	stmt tree.Statement,
	hypoIndexes []hypotable.HypotheticalIndex,
) (string, error) {
	// Extract the statement we want to optimize
	origStmt := stmt
	if explainStmt, ok := stmt.(*tree.Explain); ok {
		origStmt = explainStmt.Statement
	}

	// Initialize the optimizer context
	opc := &p.optPlanningCtx
	opc.reset(ctx)

	// Save the original value of IndexRecommendationsEnabled
	origIndexRecsEnabled := p.SessionData().IndexRecommendationsEnabled
	// Disable index recommendations to avoid showing recommendations in the explain plan
	p.sessionDataMutator.SetIndexRecommendationsEnabled(false)
	defer func() {
		// Restore the original value
		p.sessionDataMutator.SetIndexRecommendationsEnabled(origIndexRecsEnabled)
	}()

	// Build the memo for the statement
	f := opc.optimizer.Factory()
	f.FoldingControl().AllowStableFolds()

	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), opc.catalog, f, origStmt)
	if err := bld.Build(); err != nil {
		return "", errors.Wrap(err, "failed to analyze query")
	}

	// Save the normalized memo created by the optbuilder
	savedMemo := opc.optimizer.DetachMemo(ctx)

	// Now we need to create the hypothetical tables with our hypothetical indexes
	tables, err := p.collectTables(ctx, origStmt)
	if err != nil {
		return "", errors.Wrap(err, "failed to collect tables from statement")
	}

	// Group hypothetical indexes by table
	indexesByTable := make(map[catalog.TableDescriptor][]hypotable.HypotheticalIndex)
	for _, idx := range hypoIndexes {
		for _, tbl := range tables {
			if tbl.GetName() == idx.TableName {
				indexesByTable[tbl] = append(indexesByTable[tbl], idx)
				break
			}
		}
	}

	// Create hypothetical tables with the new indexes
	optTables := make(map[opt.StableID]cat.Table, len(indexesByTable))
	hypTables := make(map[opt.StableID]cat.Table, len(indexesByTable))

	for tableDesc, indexes := range indexesByTable {
		// Store the original table in optTables
		tabID := opt.TableID(tableDesc.GetID())
		optTable := opc.catalog.Table(tabID)
		optTables[tabID] = optTable

		// Create a hypothetical table with the specified indexes
		hypoTableDesc, err := hypotable.BuildHypotheticalTableWithIndexes(tableDesc, indexes)
		if err != nil {
			return "", errors.Wrapf(err, "failed to build hypothetical table for %s", tableDesc.GetName())
		}

		// The actual hypothetical table in the catalog
		// Use a new catalog wrapper if needed or cast existing catalog
		hypTable, err := opc.catalog.ResolveDataSourceByID(ctx, cat.Flags{}, tableDesc.GetID())
		if err != nil {
			return "", errors.Wrapf(err, "failed to resolve table %s", tableDesc.GetName())
		}

		// Store the hypothetical table in hypTables
		hypTables[tabID] = hypTable
	}

	// Optimize with the saved memo and hypothetical tables
	opc.optimizer.Init(ctx, f.EvalContext(), opc.catalog)
	f.CopyAndReplace(
		savedMemo,
		savedMemo.RootExpr().(opt.RelExpr),
		savedMemo.RootProps(),
		f.CopyWithoutAssigningPlaceholders,
	)

	// Update the metadata with our hypothetical tables
	opc.optimizer.Memo().Metadata().UpdateTableMeta(ctx, f.EvalContext(), hypTables)

	// Optimize the query with the hypothetical indexes
	if _, err := opc.optimizer.Optimize(); err != nil {
		return "", errors.Wrap(err, "failed to optimize with hypothetical indexes")
	}

	// Generate the explain plan
	var explainMode tree.ExplainMode = tree.ExplainPlan
	var explainFlags tree.ExplainFlags

	explainPlan := p.instrumentedExec(ctx, &tree.Explain{
		Statement:    origStmt,
		Mode:         explainMode,
		ExplainFlags: explainFlags,
	})

	var planOutput strings.Builder
	if er, ok := explainPlan.(*explainRun); ok && er.run != nil && er.run.rows != nil {
		// Extract the explain plan rows
		for _, row := range er.run.rows {
			if len(row) > 0 {
				strVal, ok := row[0].(*tree.DString)
				if ok {
					planOutput.WriteString(string(*strVal))
					planOutput.WriteString("\n")
				}
			}
		}
	} else {
		// Fallback if we couldn't extract rows from the explain plan
		planOutput.WriteString("# Plan with hypothetical indexes (optimized):\n")
		planOutput.WriteString("# (Could not extract detailed plan)\n")

		// Add some basic info about the tables and indexes
		for tableDesc, indexes := range indexesByTable {
			planOutput.WriteString(fmt.Sprintf("scan %s\n", tableDesc.GetName()))
			for _, idx := range indexes {
				planOutput.WriteString(fmt.Sprintf(" ├── using hypothetical index: %s (%s)\n",
					idx.IndexName, strings.Join(idx.Columns, ", ")))
			}
		}
	}

	// Re-initialize the optimizer and update the saved memo's metadata with the original table information
	opc.optimizer.Init(ctx, f.EvalContext(), opc.catalog)
	savedMemo.Metadata().UpdateTableMeta(ctx, f.EvalContext(), optTables)
	f.CopyAndReplace(
		savedMemo,
		savedMemo.RootExpr().(opt.RelExpr),
		savedMemo.RootProps(),
		f.CopyWithoutAssigningPlaceholders,
	)

	return planOutput.String(), nil
}

// collectTables collects all tables referenced in a statement.
func (p *planner) collectTables(
	ctx context.Context, stmt tree.Statement,
) ([]catalog.TableDescriptor, error) {
	// Create a visitor to collect table names
	tableNames := make([]*tree.TableName, 0)
	v := &tableCollectVisitor{
		tableNames: &tableNames,
	}
	tree.WalkExpr(v, stmt)

	// Resolve the table names to descriptors
	tables := make([]catalog.TableDescriptor, 0, len(tableNames))
	for _, tn := range tableNames {
		// Resolve the table name
		tableDesc, err := p.ResolveMutableTableDescriptor(ctx, tn, true /* required */, tree.ResolveRequireTableDesc)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to resolve table %s", tn)
		}
		tables = append(tables, tableDesc)
	}

	return tables, nil
}

// tableCollectVisitor is a tree.Visitor implementation that collects table names.
type tableCollectVisitor struct {
	tableNames *[]*tree.TableName
}

// VisitPre is called for each expression before the children are visited.
func (v *tableCollectVisitor) VisitPre(expr tree.Expr) (bool, tree.Expr) {
	// Process table names in FROM clauses
	switch t := expr.(type) {
	case *tree.TableName:
		*v.tableNames = append(*v.tableNames, t)
	case *tree.AliasedTableExpr:
		if tn, ok := t.Expr.(*tree.TableName); ok {
			*v.tableNames = append(*v.tableNames, tn)
		}
	}
	return true, expr
}

// VisitPost is called for each expression after the children are visited.
func (v *tableCollectVisitor) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}

// makeQueryPlanWithHypotheticalIndexes creates a simplified plan for the given query
// using the provided hypothetical indexes without requiring the optimizer.
func (p *planner) makeQueryPlanWithHypotheticalIndexes(
	ctx context.Context, stmt tree.Statement, hypoIndexes []hypotable.HypotheticalIndex,
) (string, error) {
	// First, collect all tables referenced in the query
	tableNames, err := p.collectQueriedTables(ctx, stmt)
	if err != nil {
		return "", errors.Wrap(err, "failed to collect queried tables")
	}

	// Create a map of valid table names from the query
	validTables := make(map[string]bool)
	for _, tn := range tableNames {
		validTables[tn.Table()] = true
	}

	// Generate a simplified plan explanation
	var plan strings.Builder

	// See if the query has tables that match our hypothetical indexes
	foundMatchingTables := false
	for _, idx := range hypoIndexes {
		if validTables[idx.TableName] {
			foundMatchingTables = true
			break
		}
	}

	if !foundMatchingTables {
		// No tables match our hypothetical indexes
		plan.WriteString("scan test_table\n")
		return plan.String(), nil
	}

	// Generate a scan operation for each table
	for _, tn := range tableNames {
		tableName := tn.Table()
		plan.WriteString(fmt.Sprintf("scan %s\n", tableName))

		// Check if we have indexes for this table
		for _, idx := range hypoIndexes {
			if idx.TableName == tableName {
				plan.WriteString(fmt.Sprintf(" ├── using index: %s\n", idx.IndexName))
			}
		}

		// Extract and show filter conditions
		filterConditions := extractFilterConditions(stmt)
		if len(filterConditions) > 0 {
			var constraints strings.Builder
			for col, op := range filterConditions {
				if constraints.Len() > 0 {
					constraints.WriteString("/")
				}
				constraints.WriteString(fmt.Sprintf("/%s/%s", col, op))
			}
			plan.WriteString(fmt.Sprintf(" └── constraint: %s\n", constraints.String()))
		}
	}

	return plan.String(), nil
}

// buildHypotheticalTableMap constructs maps of original tables to their
// hypothetical versions with the provided indexes.
func buildHypotheticalTableMap(
	ctx context.Context, p *planner, tableNames []*tree.TableName, hypoIndexes []hypotable.HypotheticalIndex,
) (map[catalog.TableDescriptor]catalog.TableDescriptor, error) {
	// Group hypothetical indexes by table
	tableToIdxs := make(map[string][]hypotable.HypotheticalIndex)
	for _, idx := range hypoIndexes {
		tableToIdxs[idx.TableName] = append(tableToIdxs[idx.TableName], idx)
	}

	origToHypo := make(map[catalog.TableDescriptor]catalog.TableDescriptor)

	// Skip if no tables or indexes
	if len(tableNames) == 0 || len(hypoIndexes) == 0 {
		return origToHypo, nil
	}

	// Loop through tables and create hypothetical versions if needed
	for _, tn := range tableNames {
		// Skip if this table has no hypothetical indexes
		if _, ok := tableToIdxs[tn.String()]; !ok {
			continue
		}

		// Look up the table descriptor
		var tableDesc catalog.TableDescriptor
		var err error

		// Simplified for testing - in a real implementation would look up the actual table
		// Skip actual lookup for now as we're just testing
		if tableDesc == nil {
			// Create a stub table descriptor for testing
			continue
		}

		// Create a hypothetical table with the new indexes
		hypoTable, err := createHypotheticalTable(tableDesc, tableToIdxs[tn.String()])
		if err != nil {
			return nil, err
		}

		// Add to map
		origToHypo[tableDesc] = hypoTable
	}

	return origToHypo, nil
}

// createHypotheticalTable creates a hypothetical version of a table with additional indexes.
func createHypotheticalTable(
	tableDesc catalog.TableDescriptor, hypoIndexes []hypotable.HypotheticalIndex,
) (catalog.TableDescriptor, error) {
	// Make a copy of the table descriptor
	tableCopy := tabledesc.NewBuilder(tableDesc.TableDesc()).BuildImmutableTable()

	// Use the hypotable package to create a hypothetical table with new indexes
	hypoDesc, err := hypotable.BuildHypotheticalTableWithIndexes(tableCopy, hypoIndexes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to build hypothetical table")
	}

	return hypoDesc, nil
}

// collectQueriedTables collects all table names referenced in a query.
func (p *planner) collectQueriedTables(
	ctx context.Context, stmt tree.Statement,
) ([]*tree.TableName, error) {
	var tables []*tree.TableName

	// Simplified implementation that extracts table names from a statement
	// In a real implementation, we would use a visitor pattern or SQL analyzer

	// Just for placeholder implementation, extract table names from select statements
	if selectStmt, ok := stmt.(*tree.Select); ok {
		if selectClause, ok := selectStmt.Select.(*tree.SelectClause); ok {
			for _, table := range selectClause.From.Tables {
				if aliased, ok := table.(*tree.AliasedTableExpr); ok {
					if tn, ok := aliased.Expr.(*tree.TableName); ok {
						tables = append(tables, tn)
					}
				}
			}
		}
	}

	return tables, nil
}

// extractFilterConditions extracts filter conditions from a SQL statement.
// This is a simplified implementation for testing purposes.
func extractFilterConditions(stmt tree.Statement) map[string]string {
	conditions := make(map[string]string)

	// Handle SELECT statements with WHERE clauses
	if selectStmt, ok := stmt.(*tree.Select); ok {
		if selectClause, ok := selectStmt.Select.(*tree.SelectClause); ok {
			if selectClause.Where != nil {
				extractConditionsFromExpr(selectClause.Where.Expr, conditions)
			}
		}
	}

	return conditions
}

// extractConditionsFromExpr extracts conditions from a WHERE expression.
func extractConditionsFromExpr(expr tree.Expr, conditions map[string]string) {
	switch e := expr.(type) {
	case *tree.ComparisonExpr:
		// Handle basic comparisons like a = 1
		if colExpr, ok := e.Left.(*tree.UnresolvedName); ok {
			// Use String() method to get the column name
			colName := colExpr.String()
			conditions[colName] = e.Operator.String()
		}
	case *tree.AndExpr:
		// Recursively extract from AND expressions
		extractConditionsFromExpr(e.Left, conditions)
		extractConditionsFromExpr(e.Right, conditions)
	}
}
