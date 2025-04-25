// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// HypoIndexExplainBuiltin contains all the builtin functions relating to hypothetical indexes.
// This includes the function hypo_index_explain, which simulates the behavior of
// PostgreSQL's HypoPG extension (https://github.com/HypoPG/hypopg) to allow
// users to see the execution plan for a query as if hypothetical indexes existed,
// without actually creating them.
var HypoIndexExplainBuiltin = makeBuiltin(
	tree.FunctionProperties{
		Category:         builtinconstants.CategorySystemInfo,
		DistsqlBlocklist: true,
	},
	tree.Overload{
		Types: tree.ParamTypes{
			{Name: "query", Typ: types.String},
			{Name: "indexes", Typ: types.StringArray},
		},
		ReturnType: tree.FixedReturnType(types.String),
		Fn:         hypoIndexExplain,
		Info: `Generates an EXPLAIN plan for the provided query, using the specified 
hypothetical indexes. Each index must be provided as a valid CREATE INDEX statement.
Example: SELECT hypo_index_explain('SELECT * FROM t WHERE a > 10', ARRAY['CREATE INDEX hypo_idx ON t(a)'])`,
		Volatility: volatility.Stable,
	},
	tree.Overload{
		Types: tree.ParamTypes{
			{Name: "query", Typ: types.String},
			{Name: "indexes", Typ: types.StringArray},
			{Name: "options", Typ: types.String},
		},
		ReturnType: tree.FixedReturnType(types.String),
		Fn:         hypoIndexExplain,
		Info: `Generates an EXPLAIN plan for the provided query with additional options, 
using the specified hypothetical indexes. Each index must be provided as a valid CREATE INDEX statement.
The options must be a space-separated list of valid EXPLAIN options (e.g., 'VERBOSE TYPES').
Example: SELECT hypo_index_explain('SELECT * FROM t WHERE a > 10', ARRAY['CREATE INDEX hypo_idx ON t(a)'], 'VERBOSE')`,
		Volatility: volatility.Stable,
	},
)

// hypoIndexExplain implements the builtin for hypo_index_explain.
// In Phase 2, it calls the planner's HypoIndexExplainBuiltin method if available,
// otherwise falls back to the Phase 1 implementation that returns a constant string.
func hypoIndexExplain(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
	query := string(tree.MustBeDString(args[0]))
	if query == "" {
		return nil, pgerror.New(
			pgcode.InvalidParameterValue,
			"query cannot be empty",
		)
	}

	indexesArray, ok := args[1].(*tree.DArray)
	if !ok {
		return nil, errors.AssertionFailedf("expected string array, got %T", args[1])
	}

	if indexesArray.Len() == 0 {
		return nil, pgerror.New(
			pgcode.InvalidParameterValue,
			"at least one hypothetical index must be provided",
		)
	}

	// Extract the indexes as strings
	var indexes []string
	for i := range indexesArray.Array {
		indexStr, ok := indexesArray.Array[i].(*tree.DString)
		if !ok {
			return nil, errors.AssertionFailedf("expected string, got %T", indexesArray.Array[i])
		}

		if string(*indexStr) == "" {
			return nil, pgerror.Newf(
				pgcode.InvalidParameterValue,
				"index statement at position %d cannot be empty",
				i,
			)
		}

		indexes = append(indexes, string(*indexStr))
	}

	// Check if we have the optional EXPLAIN options parameter
	var options string
	if len(args) > 2 {
		options = string(tree.MustBeDString(args[2]))
	}

	// Try to use the planner method if available
	planner, hasPlannerMethod := evalCtx.Planner.(interface {
		HypoIndexExplainBuiltin(ctx context.Context, indexDefs string, query string, explainOpts string) (string, error)
	})

	if hasPlannerMethod {
		// Join all index statements into a single string for parsing
		var indexDefsBuilder strings.Builder
		for i, idx := range indexes {
			if i > 0 {
				indexDefsBuilder.WriteString(" ")
			}

			// Add semicolon if needed
			if !strings.HasSuffix(strings.TrimSpace(idx), ";") {
				indexDefsBuilder.WriteString(idx + ";")
			} else {
				indexDefsBuilder.WriteString(idx)
			}
		}

		// Call the planner implementation (Phase 2)
		result, err := planner.HypoIndexExplainBuiltin(ctx, indexDefsBuilder.String(), query, options)
		if err != nil {
			return nil, err
		}

		return tree.NewDString(result), nil
	}

	// Fallback to Phase 1 implementation
	result := fmt.Sprintf(
		"Hypothetical EXPLAIN plan (Phase 1 - constant string):\n"+
			"Query: %s\n"+
			"Hypothetical Indexes:\n",
		query,
	)

	for i, idx := range indexes {
		result += fmt.Sprintf("  %d: %s\n", i+1, idx)
	}

	if options != "" {
		result += fmt.Sprintf("EXPLAIN Options: %s\n", options)
	}

	result += "\nFull implementation coming soon. This is a placeholder in Phase 1."

	return tree.NewDString(result), nil
}
