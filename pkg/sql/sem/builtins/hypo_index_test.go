// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

func TestHypoIndexExplain(t *testing.T) {
	evalCtx := eval.NewTestingEvalContext(nil)
	defer evalCtx.Stop(context.Background())

	testCases := []struct {
		name     string
		query    string
		indexes  []string
		options  string
		hasError bool
	}{
		{
			name:    "Basic query with one index",
			query:   "SELECT * FROM test WHERE a > 10",
			indexes: []string{"CREATE INDEX idx_a ON test(a)"},
		},
		{
			name:    "Query with multiple indexes",
			query:   "SELECT * FROM test WHERE a > 10 AND b = 'test'",
			indexes: []string{"CREATE INDEX idx_a ON test(a)", "CREATE INDEX idx_b ON test(b)"},
		},
		{
			name:    "With EXPLAIN options",
			query:   "SELECT * FROM test WHERE a > 10",
			indexes: []string{"CREATE INDEX idx_a ON test(a)"},
			options: "VERBOSE",
		},
		{
			name:     "Empty query",
			query:    "",
			indexes:  []string{"CREATE INDEX idx_a ON test(a)"},
			hasError: true,
		},
		{
			name:     "Empty indexes array",
			query:    "SELECT * FROM test WHERE a > 10",
			indexes:  []string{},
			hasError: true,
		},
		{
			name:     "Empty index string",
			query:    "SELECT * FROM test WHERE a > 10",
			indexes:  []string{""},
			hasError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			args := make(tree.Datums, 0)
			args = append(args, tree.NewDString(tc.query))

			// Create index array
			indexArray := tree.NewDArray(types.String)
			for _, idx := range tc.indexes {
				if err := indexArray.Append(tree.NewDString(idx)); err != nil {
					t.Fatal(err)
				}
			}
			args = append(args, indexArray)

			// Add options if present
			if tc.options != "" {
				args = append(args, tree.NewDString(tc.options))
			}

			// Call the function
			result, err := hypoIndexExplain(context.Background(), evalCtx, args)

			if tc.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				// Check that it's a string type
				_, ok := result.(*tree.DString)
				require.True(t, ok)
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
