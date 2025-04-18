// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// hypoIndexExplain analyzes a SQL query using hypothetical indexes.
// Args:
//   - query (string): The SQL query to analyze
//   - indexes (array): List of CREATE INDEX statements defining hypothetical indexes
//   - options (string, optional): Space-separated list of EXPLAIN options
//
// Returns: The EXPLAIN plan as if those indexes existed
func hypoIndexExplain(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
	if len(args) < 2 {
		return nil, pgerror.New(pgcode.InvalidParameterValue,
			"hypo_index_explain requires at least a query and one hypothetical index")
	}

	// Get the query string from the first argument
	query := string(tree.MustBeDString(args[0]))

	// Parse the SQL query to validate it
	stmts, err := parser.Parse(query)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.Syntax, "failed to parse query")
	}
	if len(stmts) != 1 {
		return nil, pgerror.New(pgcode.InvalidParameterValue,
			"query must contain exactly one SQL statement")
	}

	// Get the indexes array from the second argument
	indexesArray, ok := args[1].(*tree.DArray)
	if !ok {
		return nil, pgerror.New(pgcode.InvalidParameterValue,
			"second argument must be an array of CREATE INDEX statements")
	}

	// Extract and parse each CREATE INDEX statement
	indexes := make([]string, 0, indexesArray.Len())
	for i := range indexesArray.Array {
		if indexesArray.Array[i] == tree.DNull {
			continue
		}

		indexStmt, ok := tree.AsDString(indexesArray.Array[i])
		if !ok {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue,
				"array element %d is not a valid string", i+1)
		}

		// Parse the CREATE INDEX statement to validate it
		indexStmts, err := parser.Parse(string(indexStmt))
		if err != nil {
			return nil, pgerror.Wrapf(err, pgcode.Syntax,
				"failed to parse CREATE INDEX statement at index %d", i+1)
		}

		if len(indexStmts) != 1 {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue,
				"index definition at position %d must contain exactly one CREATE INDEX statement", i+1)
		}

		// Verify that this is actually a CREATE INDEX statement
		_, ok = indexStmts[0].AST.(*tree.CreateIndex)
		if !ok {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue,
				"statement at position %d is not a CREATE INDEX statement", i+1)
		}

		indexes = append(indexes, string(indexStmt))
	}

	// Check if we have at least one valid index
	if len(indexes) == 0 {
		return nil, pgerror.New(pgcode.InvalidParameterValue,
			"at least one valid CREATE INDEX statement must be provided")
	}

	// Parse EXPLAIN options if provided
	var explainOptions string
	if len(args) > 2 {
		explainOptions = string(tree.MustBeDString(args[2]))
	}

	// For this phase, just return a formatted string with what we parsed
	var result strings.Builder
	result.WriteString(fmt.Sprintf("Query: %s\n\nHypothetical Indexes:\n", query))
	for i, idx := range indexes {
		result.WriteString(fmt.Sprintf("%d: %s\n", i+1, idx))
	}

	if explainOptions != "" {
		result.WriteString(fmt.Sprintf("\nEXPLAIN options: %s\n", explainOptions))
	}

	result.WriteString("\nIn the next phase, this will be replaced with the actual EXPLAIN output.")
	return tree.NewDString(result.String()), nil
}
