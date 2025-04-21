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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// HypoIndexExplainBuiltin is the implementation of the hypo_index_explain builtin
// that uses the optimizer to generate a query plan with hypothetical indexes.
func (p *planner) HypoIndexExplainBuiltin(
	ctx context.Context, indexDefs string, query string, explainFormat string,
) (string, error) {
	explainMode, err := explainModeFromString(explainFormat)
	if err != nil {
		return "", err
	}

	// Parse the index statements
	indexStmts, err := parser.Parse(indexDefs)
	if err != nil {
		return "", errors.Wrapf(err, "failed to parse index definitions")
	}

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
		hypotheticalIndexes = append(hypotheticalIndexes, hypoIdx)
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

	// Build the optimizer plan with hypothetical indexes
	planCtx := p.NewPlanningCtx(ctx, p.ExtendedEvalContext(), nil /* stmt */, nil /* instrumentation */)
	optPlan, err := p.makeQueryPlanWithHypotheticalIndexes(ctx, planCtx, stmt.AST, hypotheticalIndexes)
	if err != nil {
		return "", err
	}

	// Generate the EXPLAIN output
	res, err := getPlanDescription(
		ctx, p.extendedEvalCtx.ExecCfg, p.extendedEvalCtx.ExecCfg.DistSQLPlanner, optPlan,
		p.curPlan.planComponents.subqueryPlans, p.ExtendedEvalContext(), explainMode, false, /* includePlanRegions */
	)
	if err != nil {
		return "", errors.Wrap(err, "failed to explain plan")
	}

	return res, nil
}

// buildHypotheticalIndexFromStatement converts a CREATE INDEX statement into a HypotheticalIndex structure.
func buildHypotheticalIndexFromStatement(
	createIndexStmt *tree.CreateIndex,
) (hypotable.HypotheticalIndex, error) {
	if createIndexStmt.Inverted {
		return hypotable.HypotheticalIndex{}, pgerror.New(pgcode.InvalidParameterValue,
			"inverted indexes are not supported yet for hypothetical indexes")
	}

	// Initialize the ID by hashing the table name and index name
	var directionsBackward []bool
	var columnNames []string

	if createIndexStmt.PartitionBy != nil {
		return hypotable.HypotheticalIndex{}, pgerror.New(pgcode.InvalidParameterValue,
			"partition by is not supported for hypothetical indexes")
	}

	// Extract column names and directions
	for _, elem := range createIndexStmt.Columns {
		// Validate direction is either ascending or descending
		if elem.Direction != tree.Ascending && elem.Direction != tree.Descending {
			return hypotable.HypotheticalIndex{}, pgerror.Newf(pgcode.InvalidParameterValue,
				"unsupported direction: %s", elem.Direction)
		}

		// We support only simple column references, not expressions
		if _, ok := elem.Column.(*tree.UnresolvedName); !ok {
			return hypotable.HypotheticalIndex{}, pgerror.New(pgcode.InvalidParameterValue,
				"only simple column references are supported in hypothetical indexes")
		}

		columnNames = append(columnNames, elem.Column.String())
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
		return tree.ExplainTree, nil
	case "plan":
		return tree.ExplainPlan, nil
	case "types":
		return tree.ExplainTypes, nil
	case "opt":
		return tree.ExplainOpt, nil
	case "opt-verbose":
		return tree.ExplainOptVerbose, nil
	case "vec":
		return tree.ExplainVec, nil
	case "distsql":
		return tree.ExplainDistSQL, nil
	case "analyze":
		return tree.ExplainAnalyze, nil
	case "ddlgraph":
		return tree.ExplainDDLGraph, nil
	}
	return 0, pgerror.Newf(pgcode.InvalidParameterValue,
		"unsupported EXPLAIN format: %s", format)
}

// makeQueryPlanWithHypotheticalIndexes creates an optimized plan for the given query
// using the provided hypothetical indexes.
func (p *planner) makeQueryPlanWithHypotheticalIndexes(
	ctx context.Context, planCtx *PlanningCtx, stmt tree.Statement, hypoIndexes []hypotable.HypotheticalIndex,
) (memo.ExprView, error) {
	// Build the original query plan
	var origPlan memo.ExprView
	var origErr error
	execFactory := p.extendedEvalCtx.ExecCfg.DefaultExecFactory()

	// First, we collect all the tables referenced in the query
	tableNames, err := p.collectQueriedTables(ctx, stmt)
	if err != nil {
		return memo.ExprView{}, errors.Wrap(err, "failed to collect queried tables")
	}

	// Build maps of original tables to hypothetical versions
	origToHypo, err := buildHypotheticalTableMap(ctx, p, tableNames, hypoIndexes)
	if err != nil {
		return memo.ExprView{}, err
	}

	// Create a modified catalog that includes hypothetical indexes
	var optsBuilder optbuilder.SemaCtx = p.semaCtx
	catalog := optCatalog{
		&p.semaCtx.Catalog,
		origToHypo,
	}
	optsBuilder.Catalog = &catalog

	ob := newOptBuilder(planCtx, &p.semaCtx, p.extendedEvalCtx)
	if ob == nil {
		return memo.ExprView{}, errors.New("failed to create optimizer")
	}

	origPlan, origErr = ob.buildExpr(ctx, stmt, &p.semaCtx, p.ExtendedEvalContext())
	if origErr != nil {
		return memo.ExprView{}, errors.Wrap(origErr, "failed to build query plan")
	}

	// Generate a physical plan using the memo with hypothetical indexes.
	execCfg := p.extendedEvalCtx.ExecCfg
	plh := execCfg.DistSQLPlanner.NewPhysPlanHook(
		planCtx, &p.curPlan.planComponents.subqueryPlans,
		execFactory,
	)
	if err := plh.BeforeBuildPhysicalPlan(ctx, &p.semaCtx, p.ExtendedEvalContext()); err != nil {
		return memo.ExprView{}, err
	}

	return origPlan, nil
}

// buildHypotheticalTableMap constructs maps of original tables to their
// hypothetical versions with the provided indexes.
func buildHypotheticalTableMap(
	ctx context.Context, p *planner, tableNames []*tree.TableName, hypoIndexes []hypotable.HypotheticalIndex,
) (map[catalog.TableDescriptor]catalog.TableDescriptor, error) {
	origToHypo := make(map[catalog.TableDescriptor]catalog.TableDescriptor)

	// Skip if no tables or indexes
	if len(tableNames) == 0 || len(hypoIndexes) == 0 {
		return origToHypo, nil
	}

	// Group hypothetical indexes by table
	tableToIdxs := make(map[string][]hypotable.HypotheticalIndex)
	for _, idx := range hypoIndexes {
		tableToIdxs[idx.TableName] = append(tableToIdxs[idx.TableName], idx)
	}

	// Loop through tables and create hypothetical versions if needed
	for _, tn := range tableNames {
		// Skip if this table has no hypothetical indexes
		if _, ok := tableToIdxs[tn.String()]; !ok {
			continue
		}

		// Look up the table descriptor
		tableDesc, err := p.Descriptors().GetImmutableTableByName(
			ctx, p.txn, tn, tree.ObjectLookupFlags{
				CommonLookupFlags: tree.CommonLookupFlags{
					Required:       true,
					AvoidLeased:    p.avoidCachedDescriptors,
					IncludeOffline: true,
				},
			},
		)
		if err != nil {
			return nil, err
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
	tableNameVisitor := &tree.TableNameVisitor{
		TableNameVisitorFunc: func(tn *tree.TableName, _ tree.CommentEscapeSyntax) {
			if tn != nil {
				tables = append(tables, tn)
			}
		},
		ParentStatement: stmt,
	}
	tableNameVisitor.Visit(nil)
	return tables, nil
}

// optCatalog is a catalog implementation that maps original table descriptors to
// hypothetical versions with new indexes.
type optCatalog struct {
	catalog.Catalog
	origToHypo map[catalog.TableDescriptor]catalog.TableDescriptor
}

func (c optCatalog) ResolveTableDescriptor(
	ctx context.Context, flags tree.ObjectLookupFlags, tn *tree.TableName,
) (catalog.TableDescriptor, error) {
	desc, err := c.Catalog.ResolveTableDescriptor(ctx, flags, tn)
	if err != nil {
		return nil, err
	}

	// Check if we have a hypothetical version of this table
	if hypoDesc, ok := c.origToHypo[desc]; ok {
		return hypoDesc, nil
	}

	return desc, nil
}

// getPlanDescription generates a human-readable representation of the plan tree.
func getPlanDescription(
	ctx context.Context,
	execCfg any,
	dsp any,
	ev memo.ExprView,
	subqueryPlans []subquery,
	evalCtx *eval.Context,
	explainMode tree.ExplainMode,
	includePlanRegions bool,
) (string, error) {
	// For this initial implementation, call into the actual explain machinery
	// This will be expanded with more detailed implementation in a future PR
	return formatExplainPlan(ctx, ev, explainMode)
}

// formatExplainPlan formats the explain plan into a string representation.
// This is a simplified implementation that will be expanded in the future.
func formatExplainPlan(
	ctx context.Context, plan memo.ExprView, explainMode tree.ExplainMode,
) (string, error) {
	var result strings.Builder
	result.WriteString("EXPLAIN with hypothetical indexes:\n")

	// In a real implementation, this would use the optimizer's explain machinery to generate
	// a detailed plan. For now, we'll return a more informative placeholder that includes
	// basic plan information.

	// Add hypothetical indexes information
	result.WriteString("Using hypothetical indexes for query optimization\n")

	// Basic plan info
	result.WriteString("Plan:\n")

	// Extract basic information from the memo
	root := plan.Root()
	var scanInfo strings.Builder

	// Try to extract scan information
	if root.ChildCount() > 0 {
		for i := 0; i < root.ChildCount(); i++ {
			child := root.Child(i)
			if child.Operator().String() == "scan" {
				scanInfo.WriteString(fmt.Sprintf("  scan %s\n", child.Private().String()))
			}
		}
	}

	// If we didn't find any scan info, just use the plan string
	if scanInfo.Len() == 0 {
		result.WriteString(fmt.Sprintf("  %s\n", plan.String()))
	} else {
		result.WriteString(scanInfo.String())
	}

	return result.String(), nil
}
