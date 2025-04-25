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
	// Parse the query
	stmts, err := parser.Parse(query)
	if err != nil {
		return "", pgerror.Wrapf(err, pgcode.Syntax, "failed to parse query")
	}
	if len(stmts) != 1 {
		return "", pgerror.New(pgcode.InvalidParameterValue,
			"query must contain exactly one SQL statement")
	}

	// Parse the CREATE INDEX statements
	indexStmts, err := parser.Parse(indexDefs)
	if err != nil {
		return "", pgerror.Wrapf(err, pgcode.Syntax, "failed to parse index definitions")
	}

	// Validate that all statements are CREATE INDEX statements
	for i, stmt := range indexStmts {
		if _, ok := stmt.AST.(*tree.CreateIndex); !ok {
			return "", pgerror.Newf(pgcode.InvalidParameterValue,
				"statement at position %d is not a CREATE INDEX statement", i+1)
		}
	}

	// Set up the trace and error recovery similar to makeQueryIndexRecommendation
	origCtx := ctx
	ctx, sp := tracing.EnsureChildSpan(ctx, p.execCfg.AmbientCtx.Tracer, "hypo index explain")
	defer sp.Finish()

	var result strings.Builder
	var memoErr error

	defer func() {
		if r := recover(); r != nil {
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
	}()

	// Save a copy of the original optimizer state before we manipulate it
	// We'll need to restore this later
	originalOptimizer := p.optPlanningCtx.optimizer

	defer func() {
		// Restore the original optimizer state to avoid interference with normal query execution
		p.optPlanningCtx.optimizer = originalOptimizer
	}()

	// Create a temporary separate optimizer for our analysis
	// This prevents interference with the regular execution path
	var tempOptimizer xform.Optimizer
	tempOptimizer.Init(ctx, p.EvalContext(), p.optPlanningCtx.catalog)

	// Build the memo directly without going through makeOptimizerPlan
	f := tempOptimizer.Factory()
	f.FoldingControl().AllowStableFolds()
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), p.optPlanningCtx.catalog, f, stmts[0].AST)
	if err := bld.Build(); err != nil {
		return "", err
	}

	// Save the normalized memo created by the optbuilder
	savedMemo := tempOptimizer.DetachMemo(ctx)

	// Use the optimizer to fully optimize the memo
	f = tempOptimizer.Factory()
	f.FoldingControl().AllowStableFolds()
	f.CopyAndReplace(
		savedMemo,
		savedMemo.RootExpr().(memo.RelExpr),
		savedMemo.RootProps(),
		f.CopyWithoutAssigningPlaceholders,
	)

	// Only use normalization rules
	tempOptimizer.NotifyOnMatchedRule(func(ruleName opt.RuleName) bool {
		return ruleName.IsNormalize()
	})

	if _, err = tempOptimizer.Optimize(); err != nil {
		return "", err
	}

	// TODO: Here we would create our hypothetical indexes and tables
	// Similar to how indexrec.BuildOptAndHypTableMaps works
	// For now, we'll just format the results without applying the hypothetical indexes

	// Format the result
	result.WriteString(fmt.Sprintf("# EXPLAIN with hypothetical indexes\n"))
	result.WriteString(fmt.Sprintf("# Query: %s\n", query))
	result.WriteString(fmt.Sprintf("# Hypothetical indexes provided:\n"))

	for i, stmt := range indexStmts {
		createIdx := stmt.AST.(*tree.CreateIndex)
		result.WriteString(fmt.Sprintf("#   %d: %s\n", i+1, createIdx.String()))
	}

	// Re-initialize the optimizer to clean state
	tempOptimizer.Init(origCtx, f.EvalContext(), p.optPlanningCtx.catalog)

	// Include the EXPLAIN output
	result.WriteString("\n# Note: This is a work in progress for hypothetical index explanation.\n")
	result.WriteString("# Currently just a placeholder to demonstrate the infrastructure is working.\n")
	result.WriteString("# In the final implementation, we will create hypothetical indexes\n")
	result.WriteString("# and show the actual EXPLAIN plan with those indexes.\n")

	return result.String(), memoErr
}
