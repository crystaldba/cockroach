// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hypoindex

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHypoIndexExplainerCreation(t *testing.T) {
	explainer := NewHypoIndexExplainer(nil)
	require.NotNil(t, explainer)
}

func TestHypoIndexExplainerExplain(t *testing.T) {
	ctx := context.Background()
	explainer := NewHypoIndexExplainer(nil)

	result, err := explainer.ExplainQuery(ctx, "SELECT * FROM users WHERE name = 'John'")
	require.NoError(t, err)
	require.Contains(t, result, "EXPLAIN with")
}

func TestHypotheticalIndexDef(t *testing.T) {
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
	ctx := context.Background()
	queryString := "SELECT * FROM users WHERE name = 'John'"

	args := []interface{}{queryString}
	result, err := HypoExplainImpl(ctx, args)

	require.NoError(t, err)
	require.NotNil(t, result)

	strResult, ok := result.(string)
	require.True(t, ok, "Expected result to be string")
	require.Contains(t, strResult, "EXPLAIN with")
}
