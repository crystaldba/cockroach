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

	// Create simple test context
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	ctx := context.Background()

	// Create test cases
	testCases := []struct {
		name          string
		args          tree.Datums
		expectedValue string
		expectError   bool
	}{
		{
			name: "Basic case",
			args: tree.Datums{
				tree.NewDString("SELECT * FROM test_table"), // query
				tree.NewDArray(types.String),                // array of CREATE INDEX statements
			},
			expectedValue: "hypo_index_explain stub implementation - will be replaced with actual explain output",
			expectError:   false,
		},
		{
			name: "Missing arguments",
			args: tree.Datums{
				tree.NewDString("SELECT * FROM test_table"), // only providing query
			},
			expectError: true,
		},
	}

	// Fill the array for the basic test case
	testArray := testCases[0].args[1].(*tree.DArray)
	err := testArray.Append(tree.NewDString("CREATE INDEX idx_test ON test_table (id)"))
	require.NoError(t, err)

	// Run the tests
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := hypoIndexExplain(ctx, &evalCtx, tc.args)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedValue, string(tree.MustBeDString(result)))
			}
		})
	}
}
