// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/indexrec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// HypoIndexExplainBuiltin implements the functionality for the hypo_index_explain
// builtin function. It generates an EXPLAIN plan for a query as if the specified
// hypothetical indexes existed, without actually creating them.
func (p *planner) HypoIndexExplainBuiltin(
	ctx context.Context, indexDefs string, query string, explainOpts string,
) (string, error) {
	log.Infof(ctx, "HypoIndexExplainBuiltin called with query: %s, indexDefs: %s, explainOpts: %s", query, indexDefs, explainOpts)
	// Parse the query
	stmts, err := parser.Parse(query)
	if err != nil {
		log.Warningf(ctx, "Failed to parse query: %v", err)
		return "", pgerror.Wrapf(err, pgcode.Syntax, "failed to parse query")
	}
	if len(stmts) != 1 {
		return "", pgerror.New(pgcode.InvalidParameterValue,
			"query must contain exactly one SQL statement")
	}
	log.Infof(ctx, "Query parsed successfully.")

	// Parse the CREATE INDEX statements
	indexStmts, err := parser.Parse(indexDefs)
	if err != nil {
		log.Warningf(ctx, "Failed to parse index definitions: %v", err)
		return "", pgerror.Wrapf(err, pgcode.Syntax, "failed to parse index definitions")
	}
	log.Infof(ctx, "Index definitions parsed successfully.")

	// Validate that all statements are CREATE INDEX statements
	var createIndexStmts []*tree.CreateIndex
	var tableNames []string
	for i, stmt := range indexStmts {
		createIdx, ok := stmt.AST.(*tree.CreateIndex)
		if !ok {
			log.Warningf(ctx, "Statement at position %d is not a CREATE INDEX statement: %s", i+1, stmt.AST.String())
			return "", pgerror.Newf(pgcode.InvalidParameterValue,
				"statement at position %d is not a CREATE INDEX statement", i+1)
		}
		createIndexStmts = append(createIndexStmts, createIdx)
		tableNames = append(tableNames, createIdx.Table.String())
	}
	log.Infof(ctx, "All index definitions are valid CREATE INDEX statements.")

	// Check if the tables exist in the database
	log.Infof(ctx, "Checking if tables exist in the database")
	for _, tableName := range tableNames {
		// Try to resolve table name
		tn := tree.MakeUnqualifiedTableName(tree.Name(tableName))
		_, err := p.ResolveTableName(ctx, &tn)
		if err != nil {
			log.Warningf(ctx, "Table %s does not exist in the database: %v", tableName, err)
			return fmt.Sprintf("# EXPLAIN with hypothetical indexes\n"+
				"# Query: %s\n"+
				"# Hypothetical indexes provided:\n"+
				"# Warning: Table '%s' not found in the database\n\n"+
				"# Execution Plan:\n"+
				"No plan available - some tables in the index definitions do not exist.",
				query, tableName), nil
		}
		log.Infof(ctx, "Table %s exists in the database", tableName)
	}

	// Parse EXPLAIN options
	var opts explain.Flags
	opts.Verbose = true // Default to verbose output
	// TODO: Parse explainOpts string to set flags appropriately

	// Call our main function to generate the plan with hypothetical indexes
	planOutput, err := makeQueryPlanWithHypotheticalIndexesOpt(ctx, p, stmts[0].AST, createIndexStmts, opts)
	if err != nil {
		log.Warningf(ctx, "Error in makeQueryPlanWithHypotheticalIndexesOpt: %v", err)
		return "", err
	}
	log.Infof(ctx, "Plan output generated successfully with length: %d", len(planOutput))

	// Format the result
	var result strings.Builder
	result.WriteString(fmt.Sprintf("# EXPLAIN with hypothetical indexes\n"))
	result.WriteString(fmt.Sprintf("# Query: %s\n", query))
	result.WriteString(fmt.Sprintf("# Hypothetical indexes provided:\n"))

	for i, createIdx := range createIndexStmts {
		result.WriteString(fmt.Sprintf("#   %d: %s\n", i+1, createIdx.String()))
	}

	// Include the explain output
	result.WriteString("\n# Execution Plan:\n")
	result.WriteString(planOutput)

	log.Infof(ctx, "Final result string length: %d", result.Len())
	return result.String(), nil
}

// makeQueryPlanWithHypotheticalIndexesOpt generates an EXPLAIN plan for a query
// considering the specified hypothetical indexes.
func makeQueryPlanWithHypotheticalIndexesOpt(
	ctx context.Context,
	p *planner,
	stmt tree.Statement,
	createIndexStmts []*tree.CreateIndex,
	explainOpts explain.Flags,
) (string, error) {
	// Set up the trace for debugging
	// origCtx := ctx
	ctx, sp := tracing.EnsureChildSpan(ctx, p.execCfg.AmbientCtx.Tracer, "hypo index explain")
	defer sp.Finish()
	log.Infof(ctx, "Started tracing in makeQueryPlanWithHypotheticalIndexesOpt")

	var memoErr error
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate internal errors without having to add
			// error checks everywhere throughout the code. This is only possible
			// because the code does not update shared state and does not manipulate
			// locks.
			if ok, e := errorutil.ShouldCatch(r); ok {
				memoErr = e
				log.Warningf(ctx, "Recovered from panic in makeQueryPlanWithHypotheticalIndexesOpt: %v", e)
				log.VEventf(ctx, 1, "%v", memoErr)
			} else {
				// Other panic objects can't be considered "safe" and thus are
				// propagated as crashes that terminate the session.
				log.Warningf(ctx, "Unhandled panic in makeQueryPlanWithHypotheticalIndexesOpt: %v", r)
				panic(r)
			}
		}
	}()

	// Save the original optimizer state
	log.Infof(ctx, "Saving original optimizer state")
	originalOptimizer := p.optPlanningCtx.optimizer
	defer func() {
		// Restore the original optimizer state
		log.Infof(ctx, "Restoring original optimizer state")
		p.optPlanningCtx.optimizer = originalOptimizer
	}()

	// Save the original index recommendations state and temporarily disable it
	log.Infof(ctx, "Disabling index recommendations")
	origIndexRecsEnabled := p.SessionData().IndexRecommendationsEnabled
	defer func() {
		// Restore the original index recommendations state
		log.Infof(ctx, "Restoring original index recommendations state")
		sessionData := p.SessionData()
		sessionData.IndexRecommendationsEnabled = origIndexRecsEnabled
	}()

	// Disable index recommendations for this operation
	sessionData := p.SessionData()
	sessionData.IndexRecommendationsEnabled = false

	// Create a new optimizer for our operation
	log.Infof(ctx, "Initializing temporary optimizer")
	var tempOptimizer xform.Optimizer
	tempOptimizer.Init(ctx, p.EvalContext(), p.optPlanningCtx.catalog)

	// Build the memo
	log.Infof(ctx, "Building initial memo")
	f := tempOptimizer.Factory()
	f.FoldingControl().AllowStableFolds()
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), p.optPlanningCtx.catalog, f, stmt)
	if err := bld.Build(); err != nil {
		log.Warningf(ctx, "Error building memo: %v", err)
		return "", err
	}
	log.Infof(ctx, "Initial memo built successfully")

	// Save the normalized memo created by the optbuilder
	log.Infof(ctx, "Detaching memo")
	savedMemo := tempOptimizer.DetachMemo(ctx)

	// Prepare to fully optimize the memo to identify tables and extract schema info
	log.Infof(ctx, "Preparing for full optimization")
	f = tempOptimizer.Factory()
	f.FoldingControl().AllowStableFolds()
	f.CopyAndReplace(
		savedMemo,
		savedMemo.RootExpr().(memo.RelExpr),
		savedMemo.RootProps(),
		f.CopyWithoutAssigningPlaceholders,
	)

	// Only use normalization rules for this first pass
	log.Infof(ctx, "Setting up normalization rules")
	tempOptimizer.NotifyOnMatchedRule(func(ruleName opt.RuleName) bool {
		isNormalize := ruleName.IsNormalize()
		log.VEventf(ctx, 2, "Optimizer rule matched: %s (IsNormalize: %v)", ruleName, isNormalize)
		return isNormalize
	})

	// Optimize to extract schema information
	log.Infof(ctx, "Running first optimization pass")
	if _, err := tempOptimizer.Optimize(); err != nil {
		log.Warningf(ctx, "Error during first optimization pass: %v", err)
		return "", err
	}
	log.Infof(ctx, "First optimization pass completed")

	// Convert the CREATE INDEX statements into index candidates
	// that can be used with the indexrec infrastructure
	log.Infof(ctx, "Creating index candidates map")
	indexCandidates := make(map[cat.Table][][]cat.IndexColumn)

	// Process each CREATE INDEX statement
	for i, createIdx := range createIndexStmts {
		// Get table name from CREATE INDEX statement
		tableName := createIdx.Table.String()
		log.Infof(ctx, "Processing CREATE INDEX #%d for table %s", i+1, tableName)

		// Find the corresponding table in the metadata
		var targetTable cat.Table
		md := f.Metadata()

		// Log metadata information for debugging
		tableCount := md.NumTables()
		log.Infof(ctx, "Current memo has %d tables", tableCount)

		// Search through all tables in the metadata using a safer approach
		tableFound := false

		// Now safely check each valid table
		for _, table_meta := range md.AllTables() {
			table := table_meta.Table
			// Get the name of the table using the catalog
			tn, err := p.optPlanningCtx.catalog.FullyQualifiedName(ctx, table)
			if err != nil {
				log.Warningf(ctx, "Error getting name for table ID %d: %v", table_meta.MetaID, err)
				continue // Skip tables we can't name
			}

			// Get the table name as a string for logging
			tableFQN := tn.String()
			log.Infof(ctx, "Examining table ID %d: %s", table_meta.MetaID, tableFQN)

			// Get just the table part for comparison (not the schema)
			tableUnqualifiedName := tn.Table()

			// Compare both fully qualified and unqualified names
			if tableFQN == tableName || tableUnqualifiedName == tableName {
				targetTable = table
				tableFound = true
				log.Infof(ctx, "Found matching table %s with ID %d", tableFQN, table_meta.MetaID)
				break
			}
		}

		if !tableFound {
			log.Warningf(ctx, "Table %s not found in query metadata - skipping index", tableName)
			continue
		}

		// Create index columns for this table
		var indexCols []cat.IndexColumn
		for _, columnDef := range createIdx.Columns {
			colName := string(columnDef.Column)
			log.Infof(ctx, "Processing column %s", colName)
			// Find column in table
			colFound := false
			for k := 0; k < targetTable.ColumnCount(); k++ {
				col := targetTable.Column(k)
				if string(col.ColName()) == colName {
					indexCol := cat.IndexColumn{Column: col}
					indexCols = append(indexCols, indexCol)
					colFound = true
					log.Infof(ctx, "Found column %s at position %d", colName, k)
					break
				}
			}
			if !colFound {
				log.Warningf(ctx, "Column %s not found in table %s", colName, tableName)
			}
		}

		if len(indexCols) > 0 {
			indexCandidates[targetTable] = append(indexCandidates[targetTable], indexCols)
			log.Infof(ctx, "Added index candidate with %d columns for table %s", len(indexCols), tableName)
		} else {
			log.Warningf(ctx, "No valid columns found for index on table %s", tableName)
		}
	}

	// Build hypothetical tables with the candidate indexes
	log.Infof(ctx, "Building hypothetical tables with index candidates")
	_, hypTables := indexrec.BuildOptAndHypTableMaps(p.optPlanningCtx.catalog, indexCandidates)
	log.Infof(ctx, "Built %d hypothetical tables", len(hypTables))

	// Skip the rest if no hypothetical tables were created
	if len(hypTables) == 0 {
		log.Warningf(ctx, "No hypothetical tables were created, nothing to explain")
		// Return an empty plan with a note about the issue
		return fmt.Sprintf("No valid hypothetical indexes could be created for the query tables.\n" +
			"Make sure the table(s) in your CREATE INDEX statements are used in the query.\n" +
			"Table names must match exactly including case and schema qualification."), nil
	}

	// Re-initialize the optimizer and update the table metadata with hypothetical tables
	log.Infof(ctx, "Re-initializing optimizer with hypothetical tables")
	tempOptimizer.Init(ctx, f.EvalContext(), p.optPlanningCtx.catalog) // Using ctx instead of origCtx

	// Safely copy and replace the memo
	f = tempOptimizer.Factory()
	f.FoldingControl().AllowStableFolds()

	// Check if savedMemo or its root expr is nil
	if savedMemo == nil {
		log.Warningf(ctx, "Saved memo is nil")
		return "Error: internal optimization error - saved memo is nil", nil
	}

	rootExpr := savedMemo.RootExpr()
	if rootExpr == nil {
		log.Warningf(ctx, "Saved memo root expression is nil")
		return "Error: internal optimization error - saved memo root expression is nil", nil
	}

	// Type assert with safety check
	relExpr, ok := rootExpr.(memo.RelExpr)
	if !ok {
		log.Warningf(ctx, "Root expression is not a RelExpr: %T", rootExpr)
		return fmt.Sprintf("Error: internal optimization error - unexpected root expression type: %T", rootExpr), nil
	}

	// Now do the copy and replace
	f.CopyAndReplace(
		savedMemo,
		relExpr,
		savedMemo.RootProps(),
		f.CopyWithoutAssigningPlaceholders,
	)

	// Update metadata to use hypothetical tables
	log.Infof(ctx, "Updating metadata with hypothetical tables")
	f.Memo().Metadata().UpdateTableMeta(ctx, f.EvalContext(), hypTables) // Using ctx instead of origCtx

	// Optimize the memo with the hypothetical indexes
	log.Infof(ctx, "Running final optimization with hypothetical indexes")
	if _, err := tempOptimizer.Optimize(); err != nil {
		log.Warningf(ctx, "Error during final optimization: %v", err)
		return "", err
	}
	log.Infof(ctx, "Final optimization completed")

	// Generate the explain plan
	log.Infof(ctx, "Generating explain plan")
	var planOutput string

	// Wrap the explain plan generation in a panic handler
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Warningf(ctx, "Panic while generating explain plan: %v", r)
				planOutput = fmt.Sprintf("Error generating explain plan: %v\n", r)
			}
		}()

		var err error
		planOutput, err = formatExplainPlan(ctx, p, &tempOptimizer, stmt, f.Memo(), explainOpts)
		if err != nil {
			log.Warningf(ctx, "Error formatting explain plan: %v", err)
			planOutput = fmt.Sprintf("Error formatting explain plan: %v\n", err)
		}
	}()

	log.Infof(ctx, "Explain plan generated with length: %d", len(planOutput))
	return planOutput, memoErr
}

// formatExplainPlan generates a formatted explanation plan for the optimized query
func formatExplainPlan(
	ctx context.Context,
	p *planner,
	optimizer *xform.Optimizer, // Pass the optimizer used
	stmtAST tree.Statement,
	memo *memo.Memo,
	opts explain.Flags,
) (string, error) {
	log.Infof(ctx, "Formatting explain plan...")

	var buf strings.Builder
	buf.WriteString("Optimizer plan with hypothetical indexes:\n\n")

	// Get optimizer root expression
	root := memo.RootExpr()
	if root == nil {
		return "Error: memo has no root expression", nil
	}

	// Extract cost from the root expression if it's been optimized
	if memo.IsOptimized() {
		// The memo is optimized when memo.rootExpr is a RelExpr with physical properties assigned
		// When that's the case, we can get the cost directly from memo.RootExpr()
		// Create a formatted string for the cost

		// Find a cleaner way to print the cost
		ok, costEstimate := memo.GetCost()
		if ok {
			buf.WriteString(fmt.Sprintf("Optimizer Cost: %f\n\n", costEstimate.C))
		} else {
			buf.WriteString("Optimizer Cost: Available, but not possible to extract\n\n")
		}
	} else {
		buf.WriteString("Optimizer Cost: Not available (plan not fully optimized)\n\n")
	}

	// Extract basic info about the plan
	buf.WriteString(fmt.Sprintf("Plan Type: %T\n", root))

	// Create a simple text output with the available information
	md := memo.Metadata()
	if md == nil {
		return "Error: memo has no metadata", nil
	}

	// Add table information
	tableCount := md.NumTables()
	buf.WriteString(fmt.Sprintf("Tables in query: %d\n\n", tableCount))

	// Track which indexes might be used for each table
	usedIndexes := make(map[string][]string)

	// Go through each table
	for _, table_meta := range md.AllTables() {
		table := table_meta.Table
		if table == nil {
			continue
		}

		// Try to get table name
		var tableName string
		tn, err := p.optPlanningCtx.catalog.FullyQualifiedName(ctx, table)
		if err != nil {
			tableName = fmt.Sprintf("Unknown-%d", table_meta.MetaID)
		} else {
			tableName = tn.String()
		}

		// Report table indexes
		indexCount := table.IndexCount()
		buf.WriteString(fmt.Sprintf("Table: %s, Indexes: %d\n", tableName, indexCount))

		// Output the first few indexes
		tableIndexes := make([]string, 0, indexCount)
		for idx := 0; idx < indexCount && idx < 10; idx++ {
			index := table.Index(idx)
			indexName := index.Name()
			colCount := index.ColumnCount()

			// Skip if index has no columns
			if colCount == 0 {
				continue
			}

			// Check if this is a hypothetical index by checking if table is hypothetical
			// and if the index is beyond the standard indexes
			indexType := ""
			if table.IsHypothetical() && idx >= table.DeletableIndexCount() {
				indexType = " (hypothetical)"
			}

			buf.WriteString(fmt.Sprintf("  Index %d: %s%s (", idx, indexName, indexType))

			// Build a string showing index columns
			var indexInfo strings.Builder
			indexInfo.WriteString(fmt.Sprintf("%s%s", indexName, indexType))
			indexInfo.WriteString(" (")

			// Output index columns
			for col := 0; col < colCount && col < 5; col++ {
				if col > 0 {
					buf.WriteString(", ")
					indexInfo.WriteString(", ")
				}
				indexCol := index.Column(col)
				colName := string(indexCol.Column.ColName())
				buf.WriteString(colName)
				indexInfo.WriteString(colName)
			}

			// If we didn't show all columns, indicate that
			if colCount > 5 {
				buf.WriteString(", ...")
				indexInfo.WriteString(", ...")
			}

			buf.WriteString(")\n")
			indexInfo.WriteString(")")

			// Store the index for this table
			if indexType != "" {
				tableIndexes = append(tableIndexes, indexInfo.String())
			}
		}
		buf.WriteString("\n")

		// Store the indexes for this table
		if len(tableIndexes) > 0 {
			usedIndexes[tableName] = tableIndexes
		}
	}

	// Use memo's String method to get a simple representation of the optimizer's plan
	buf.WriteString("Optimizer's Selected Plan:\n")

	// Create a formatted representation of the plan
	buf.WriteString(fmt.Sprintf("%v\n", root))

	// Add details about which indexes were considered
	buf.WriteString("\nNote: This execution plan shows the optimizer's plan using hypothetical indexes.\n")
	buf.WriteString("The plan cost and structure reflect how the query would be executed if these indexes existed.\n")

	result := buf.String()
	log.Infof(ctx, "Created optimizer plan explanation with length: %d", len(result))
	return result, nil
}
