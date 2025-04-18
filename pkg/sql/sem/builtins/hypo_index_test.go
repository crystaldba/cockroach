//go:build builtins_test
// +build builtins_test

// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestHypoIndexExplain(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)

	testCases := []struct {
		name        string
		query       string
		indexes     []string
		options     string
		expectError bool
	}{
		{
			name:    "Simple query with one index",
			query:   "SELECT * FROM test_table WHERE a = 1",
			indexes: []string{"CREATE INDEX idx_a ON test_table (a)"},
		},
		{
			name:  "Multiple indexes",
			query: "SELECT * FROM test_table WHERE a = 1 AND b = 2",
			indexes: []string{
				"CREATE INDEX idx_a ON test_table (a)",
				"CREATE INDEX idx_b ON test_table (b)",
				"CREATE INDEX idx_ab ON test_table (a, b)",
			},
		},
		{
			name:    "With EXPLAIN options",
			query:   "SELECT * FROM test_table WHERE a = 1",
			indexes: []string{"CREATE INDEX idx_a ON test_table (a)"},
			options: "VERBOSE",
		},
		{
			name:        "Invalid query",
			query:       "SELECT * FROM",
			indexes:     []string{"CREATE INDEX idx_a ON test_table (a)"},
			expectError: true,
		},
		{
			name:        "Invalid index statement",
			query:       "SELECT * FROM test_table",
			indexes:     []string{"CREATE INDX idx_a ON test_table (a)"},
			expectError: true,
		},
		{
			name:        "Non-CREATE INDEX statement",
			query:       "SELECT * FROM test_table",
			indexes:     []string{"SELECT * FROM test_table"},
			expectError: true,
		},
		{
			name:        "Multiple statements in query",
			query:       "SELECT * FROM test_table; SELECT 1",
			indexes:     []string{"CREATE INDEX idx_a ON test_table (a)"},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Build the args
			args := make(tree.Datums, 0, 3)
			args = append(args, tree.NewDString(tc.query))

			// Create the indexes array
			indexesArray := tree.NewDArray(types.String)
			for _, idx := range tc.indexes {
				require.NoError(t, indexesArray.Append(tree.NewDString(idx)))
			}
			args = append(args, indexesArray)

			// Add options if present
			if tc.options != "" {
				args = append(args, tree.NewDString(tc.options))
			}

			// Call the function
			result, err := hypoIndexExplain(ctx, &evalCtx, args)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)

				// Check that the result contains expected substrings
				resultStr := string(tree.MustBeDString(result))
				require.Contains(t, resultStr, tc.query)
				for _, idx := range tc.indexes {
					require.Contains(t, resultStr, idx)
				}
				if tc.options != "" {
					require.Contains(t, resultStr, tc.options)
				}
			}
		})
	}
}

func TestHypoIndexExplainErrors(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)

	// Test not enough arguments
	_, err := hypoIndexExplain(ctx, &evalCtx, tree.Datums{tree.NewDString("SELECT 1")})
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires at least a query and one hypothetical index")

	// Test empty index array
	indexesArray := tree.NewDArray(types.String)
	_, err = hypoIndexExplain(ctx, &evalCtx, tree.Datums{tree.NewDString("SELECT 1"), indexesArray})
	require.Error(t, err)
	require.Contains(t, err.Error(), "at least one valid CREATE INDEX statement must be provided")
}
