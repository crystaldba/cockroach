// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hypoindex

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestHypoIndexParsing(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		query    string
		expected string
	}{
		{
			query:    "SELECT * FROM users WHERE name = 'John'",
			expected: "SELECT * FROM users WHERE name = 'John'",
		},
		{
			query:    "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
			expected: "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.query, func(t *testing.T) {
			stmt, err := parser.ParseOne(tc.query)
			require.NoError(t, err)
			require.NotNil(t, stmt)
			require.Equal(t, tc.expected, stmt.AST.String())
		})
	}
}

func TestHypotheticalIndexDef(t *testing.T) {
	defer leaktest.AfterTest(t)()

	idxDef := HypotheticalIndexDef{
		ID:          "00000000-0000-0000-0000-000000000001",
		TableSchema: "public",
		TableName:   "users",
		IndexName:   "hypo_idx_users_name",
		Columns:     []string{"name"},
		Storing:     []string{"email"},
		Unique:      false,
		Inverted:    false,
	}

	require.Equal(t, "00000000-0000-0000-0000-000000000001", idxDef.ID)
	require.Equal(t, "public", idxDef.TableSchema)
	require.Equal(t, "users", idxDef.TableName)
	require.Equal(t, "hypo_idx_users_name", idxDef.IndexName)
	require.Equal(t, []string{"name"}, idxDef.Columns)
	require.Equal(t, []string{"email"}, idxDef.Storing)
	require.False(t, idxDef.Unique)
	require.False(t, idxDef.Inverted)
}

func TestHypoExplainImpl(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	queryString := "SELECT * FROM users WHERE name = 'John'"

	args := []tree.Datum{tree.NewDString(queryString)}
	result, err := hypoExplainImpl(ctx, args)

	require.NoError(t, err)
	require.NotNil(t, result)

	dstr, ok := result.(*tree.DString)
	require.True(t, ok, "Expected result to be *tree.DString")
	require.Equal(t, "EXPLAIN with hypothetical indexes would be shown here", string(*dstr))
}
