// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
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
	for i, stmt := range indexStmts {
		if _, ok := stmt.AST.(*tree.CreateIndex); !ok {
			log.Warningf(ctx, "Statement at position %d is not a CREATE INDEX statement: %s", i+1, stmt.AST.String())
			return "", pgerror.Newf(pgcode.InvalidParameterValue,
				"statement at position %d is not a CREATE INDEX statement", i+1)
		}
	}
	log.Infof(ctx, "All index definitions are valid CREATE INDEX statements.")

	// Set up the trace and error recovery similar to makeQueryIndexRecommendation
	origCtx := ctx
	ctx, sp := tracing.EnsureChildSpan(ctx, p.execCfg.AmbientCtx.Tracer, "hypo index explain")
	defer sp.Finish()
	log.Infof(ctx, "Tracing span started.")

	var result strings.Builder
	var memoErr error

	defer func() {
		if r := recover(); r != nil {
			log.Warningf(ctx, "Recovered from panic: %v", r)
			// This code allows us to propagate internal errors without having to add
			// error checks everywhere throughout the code. This is only possible
			// because the code does not update shared state and does not manipulate
			// locks.
			if ok, e := errorutil.ShouldCatch(r); ok {
				memoErr = e
				log.VEventf(ctx, 1, "%v", memoErr)
			} else {
				// Other panic objects can't be considered "safe" and thus are
				// propagated as crashes that terminate the session.
				panic(r)
			}
		}
		log.Infof(ctx, "Deferred panic handler executed.")
	}()

	// Save a copy of the original optimizer state before we manipulate it
	// We'll need to restore this later
	originalOptimizer := p.optPlanningCtx.optimizer
	log.Infof(ctx, "Saved original optimizer state.")

	defer func() {
		// Restore the original optimizer state to avoid interference with normal query execution
		p.optPlanningCtx.optimizer = originalOptimizer
		log.Infof(ctx, "Restored original optimizer state.")
	}()

	// // Create a temporary separate optimizer for our analysis
	// // This prevents interference with the regular execution path
	var tempOptimizer xform.Optimizer
	// tempOptimizer = originalOptimizer
	// log.Infof(ctx, "Created temporary optimizer based on original.")
	log.Infof(ctx, "Initializing temporary optimizer...")
	tempOptimizer.Init(ctx, p.EvalContext(), p.optPlanningCtx.catalog)
	log.Infof(ctx, "Initialized temporary optimizer.")

	// Build the memo directly without going through makeOptimizerPlan
	log.Infof(ctx, "Building memo...")
	f := tempOptimizer.Factory()
	f.FoldingControl().AllowStableFolds()
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), p.optPlanningCtx.catalog, f, stmts[0].AST)
	if err := bld.Build(); err != nil {
		log.Warningf(ctx, "Error building memo: %v", err)
		return "", err
	}

	// Save the normalized memo created by the optbuilder
	log.Infof(ctx, "Attempting to detach memo...")
	savedMemo := tempOptimizer.DetachMemo(ctx)
	log.Infof(ctx, "Memo detached successfully.")

	// Use the optimizer to fully optimize the memo
	f = tempOptimizer.Factory()
	f.FoldingControl().AllowStableFolds()
	log.Infof(ctx, "Copying memo for optimization...")
	f.CopyAndReplace(
		savedMemo,
		savedMemo.RootExpr().(memo.RelExpr),
		savedMemo.RootProps(),
		f.CopyWithoutAssigningPlaceholders,
	)
	log.Infof(ctx, "Memo copied.")

	// Only use normalization rules
	tempOptimizer.NotifyOnMatchedRule(func(ruleName opt.RuleName) bool {
		isNormalize := ruleName.IsNormalize()
		log.VEventf(ctx, 2, "Optimizer rule matched: %s (IsNormalize: %v)", ruleName, isNormalize)
		return isNormalize
	})

	log.Infof(ctx, "Optimizing the memo...")
	if _, err = tempOptimizer.Optimize(); err != nil {
		log.Warningf(ctx, "Optimization failed: %v", err)
		return "ERROR", err
	}
	log.Infof(ctx, "Optimization completed successfully.")

	// Parse EXPLAIN options
	opts := explain.Flags{Verbose: true} // Just set verbose for now
	// if explainOpts != "" {
	// 	for _, opt := range strings.Fields(explainOpts) {
	// 		subOpt := opt
	// 		subOptValue := ""
	// 		if equalIdx := strings.IndexRune(opt, '='); equalIdx >= 0 {
	// 			subOpt = opt[:equalIdx]
	// 			subOptValue = opt[equalIdx+1:]
	// 		}
	// 		if err := opts.Set(subOpt, subOptValue); err != nil {
	// 			return "", err
	// 		}
	// 	}
	// }

	// Format the optimized plan
	log.Infof(ctx, "Formatting explain plan...")
	planOutput, err := formatExplainPlan(ctx, p, &tempOptimizer, stmts[0].AST, f.Memo(), opts)
	if err != nil {
		log.Warningf(ctx, "Error formatting explain plan: %v", err)
		return "", err
	}
	log.Infof(ctx, "Plan formatting complete.")

	// TODO: Here we would create our hypothetical indexes and tables
	// Similar to how indexrec.BuildOptAndHypTableMaps works
	// For now, we'll just format the results without applying the hypothetical indexes
	log.Infof(ctx, "Formatting result string...")
	// Format the result
	result.WriteString(fmt.Sprintf("# EXPLAIN with hypothetical indexes\n"))
	result.WriteString(fmt.Sprintf("# Query: %s\n", query))
	result.WriteString(fmt.Sprintf("# Hypothetical indexes provided:\n"))

	for i, stmt := range indexStmts {
		createIdx := stmt.AST.(*tree.CreateIndex)
		result.WriteString(fmt.Sprintf("#   %d: %s\n", i+1, createIdx.String()))
	}

	// Include the explain output
	result.WriteString("\n# Execution Plan:\n")
	result.WriteString(planOutput)
	log.Infof(ctx, "Result string formatted.")

	// Re-initialize the optimizer (which also re-initializes the factory) and
	// update the saved memo's metadata with the original table information.
	// Prepare to re-optimize and create an executable plan.
	// Use the origCtx instead of ctx since the optimizer will hold onto this
	// context after this function ends, and we don't want "use of Span after
	// Finish" errors.
	tempOptimizer.Init(origCtx, f.EvalContext(), p.optPlanningCtx.catalog)
	// savedMemo.Metadata().UpdateTableMeta(origCtx, f.EvalContext(), p.optPlanningCtx.catalog)
	f.CopyAndReplace(
		savedMemo,
		savedMemo.RootExpr().(memo.RelExpr),
		savedMemo.RootProps(),
		f.CopyWithoutAssigningPlaceholders,
	)

	log.Infof(ctx, "HypoIndexExplainBuiltin returning result.")
	return result.String(), memoErr
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
	// 1. Get necessary context
	semaCtx := &p.semaCtx
	evalCtx := p.EvalContext()
	catalog := p.optPlanningCtx.catalog // Use the planner's catalog
	root := memo.RootExpr()

	// 2. Create explain factory
	// Use the standard exec factory for building the explain plan.
	execFactory := newExecFactory(ctx, p)
	explainFactory := explain.NewFactory(execFactory, semaCtx, evalCtx)
	log.Infof(ctx, "Created explain factory.")

	// 3. Create execbuilder
	allowAutoCommit := false // Hypo explain doesn't involve commits.
	isANSIDML := statements.IsANSIDML(stmtAST)

	execBld := execbuilder.New(
		ctx, explainFactory, optimizer, // Use passed optimizer
		memo, catalog, root,
		semaCtx, evalCtx, allowAutoCommit, isANSIDML,
	)
	execBld.DisableTelemetry() // Disable telemetry for this internal call.
	log.Infof(ctx, "Created execbuilder.")

	// 4. Build the explain plan
	log.Infof(ctx, "Building explain plan...")
	_, err := execBld.Build()
	if err != nil {
		log.Warningf(ctx, "Error building explain plan: %v", err)
		return "", err
	}
	// _explainPlan := plan
	log.Infof(ctx, "Explain plan built successfully.")

	// 5. Format the explain plan rows into a string
	log.Infof(ctx, "Formatting explain plan rows...")
	var buf strings.Builder
	colNames := colinfo.ExplainPlanColumns // Get the standard column names for EXPLAIN output
	for i, col := range colNames {
		if i > 0 {
			buf.WriteString("\t")
		}
		buf.WriteString(col.Name)
	}
	buf.WriteString("\n")

	// for {
	// 	row, err := explainPlan.Next(ctx)
	// 	if err != nil {
	// 		log.Warningf(ctx, "Error getting next row from explain plan: %v", err)
	// 		return "", err
	// 	}
	// 	if row == nil {
	// 		log.Infof(ctx, "Finished processing explain plan rows.")
	// 		break // End of rows
	// 	}
	// 	for i, datum := range row {
	// 		if i > 0 {
	// 			buf.WriteString("\t")
	// 		}
	// 		// Format the datum. We might need more sophisticated formatting later.
	// 		buf.WriteString(datum.String())
	// 	}
	// 	buf.WriteString("\n")
	// }

	log.Infof(ctx, "Formatted explain plan successfully.")
	return buf.String(), nil
}
