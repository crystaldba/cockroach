// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hypoindex

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

type mockCatalog struct{}

func (m mockCatalog) ResolveDataSource(ctx context.Context, flags cat.Flags, name *tree.TableName) (cat.DataSource, cat.DataSourceName, error) {
	return nil, cat.DataSourceName{}, nil
}

func (m mockCatalog) ResolveDataSourceByID(ctx context.Context, flags cat.Flags, dataSourceID cat.StableID) (cat.DataSource, error) {
	return nil, nil
}

func (m mockCatalog) ResolveType(ctx context.Context, name *tree.TypeName) (*types.T, error) {
	return nil, nil
}

func (m mockCatalog) ResolveTypeByID(ctx context.Context, id oid.Oid) (*types.T, error) {
	return nil, nil
}

func (m mockCatalog) ResolveFunction(ctx context.Context, name *tree.FunctionName) (tree.FunctionEffects, tree.Overload, error) {
	return 0, tree.Overload{}, nil
}

func (m mockCatalog) FullyQualifiedName(ctx context.Context, ds cat.DataSource) (cat.DataSourceName, error) {
	return cat.DataSourceName{}, nil
}

// TestHypoIndexExplainerCreation verifies explainer creation.
func TestHypoIndexExplainerCreation(t *testing.T) {
	ctx := context.Background()
	evalCtx := tree.MakeTestingEvalContext(nil)
	catalog := &mockCatalog{}

	explainer := NewHypoIndexExplainer(&evalCtx, catalog)
	require.NotNil(t, explainer)
}

// TestHypotheticalIndexDef verifies the index definition struct.
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

// TestHypoExplainImpl verifies the explain implementation.
func TestHypoExplainImpl(t *testing.T) {
	ctx := context.Background()
	queryString := "SELECT * FROM users WHERE name = 'John'"

	args := []interface{}{queryString}
	result, err := HypoExplainImpl(ctx, args)

	require.NoError(t, err)
	require.NotNil(t, result)

	strResult, ok := result.(string)
	require.True(t, ok, "Expected result to be string")
	require.Contains(t, strResult, "EXPLAIN for query")
}
