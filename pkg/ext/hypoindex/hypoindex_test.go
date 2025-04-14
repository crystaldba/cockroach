// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hypoindex

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

func TestHypoIndexExplainerCreation(t *testing.T) {
	mockExecutor := &MockSQLExecutor{}
	explainer := NewHypoIndexExplainer(nil, mockExecutor)
	require.NotNil(t, explainer)
}

func TestHypoIndexExplainerExplain(t *testing.T) {
	ctx := context.Background()

	// Create mock executor that returns empty results
	mockExecutor := &MockSQLExecutor{
		QueryResults: map[string][]tree.Datums{
			// Match any query with empty results
			"": {},
		},
	}

	explainer := NewHypoIndexExplainer(nil, mockExecutor)

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

// MockSQLExecutor implements the SQLExecutor interface for testing
type MockSQLExecutor struct {
	// QueryResults stores pre-defined results for queries
	QueryResults map[string][]tree.Datums
	// Error to return when specified
	Error error
	// LastQuery stores the last executed query for verification
	LastQuery string
	// QueryCount tracks how many times queries were executed
	QueryCount int
}

// QueryBufferedEx implements the SQLExecutor interface
func (m *MockSQLExecutor) QueryBufferedEx(
	ctx context.Context,
	opName string,
	txn interface{},
	override interface{},
	query string,
	qargs ...interface{},
) ([]tree.Datums, error) {
	m.LastQuery = query
	m.QueryCount++

	if m.Error != nil {
		return nil, m.Error
	}

	// Return pre-defined results if available
	if results, ok := m.QueryResults[query]; ok {
		return results, nil
	}

	// Return empty result by default
	return []tree.Datums{}, nil
}

func TestFetchHypotheticalIndexes(t *testing.T) {
	ctx := context.Background()

	// Create mock data
	idDatum := tree.NewDString("00000000-0000-0000-0000-000000000001")
	schemaDatum := tree.NewDString("public")
	tableDatum := tree.NewDString("users")
	indexNameDatum := tree.NewDString("hypo_idx_users_name")

	// Create column array
	columnsArray := tree.NewDArray(types.String)
	_ = columnsArray.Append(tree.NewDString("name"))

	// Create storing array
	storingArray := tree.NewDArray(types.String)
	_ = storingArray.Append(tree.NewDString("email"))

	// Create boolean values
	uniqueDatum := tree.DBoolFalse
	invertedDatum := tree.DBoolFalse

	// Create a row with all the data
	row := tree.Datums{
		idDatum,
		schemaDatum,
		tableDatum,
		indexNameDatum,
		columnsArray,
		storingArray,
		uniqueDatum,
		invertedDatum,
	}

	// Create the mock executor with our test data
	mockExecutor := &MockSQLExecutor{
		QueryResults: map[string][]tree.Datums{
			// The specific query will be matched exactly
			fmt.Sprintf(`
		SELECT 
			id, 
			table_schema, 
			table_name, 
			index_name, 
			columns, 
			storing, 
			unique, 
			inverted
		FROM %s.hypo_indexes
	`, catconstants.PgExtensionSchemaName): {row},
		},
	}

	// Create the explainer with our mock
	explainer := NewHypoIndexExplainer(nil, mockExecutor)

	// Call the function we want to test
	indexes, err := explainer.fetchHypotheticalIndexes(ctx)

	// Verify results
	require.NoError(t, err)
	require.Equal(t, 1, len(indexes))
	require.Equal(t, 1, mockExecutor.QueryCount, "Should have executed exactly one query")

	// Verify the index data was correctly extracted
	idx := indexes[0]
	require.Equal(t, "00000000-0000-0000-0000-000000000001", idx.ID)
	require.Equal(t, "public", idx.TableSchema)
	require.Equal(t, "users", idx.TableName)
	require.Equal(t, "hypo_idx_users_name", idx.IndexName)
	require.Equal(t, []string{"name"}, idx.Columns)
	require.Equal(t, []string{"email"}, idx.Storing)
	require.False(t, idx.Unique)
	require.False(t, idx.Inverted)
}

func TestFetchHypotheticalIndexesError(t *testing.T) {
	ctx := context.Background()

	// Create mock executor that returns an error
	mockExecutor := &MockSQLExecutor{
		Error: fmt.Errorf("database error"),
	}

	// Create the explainer with our mock
	explainer := NewHypoIndexExplainer(nil, mockExecutor)

	// Call the function we want to test
	_, err := explainer.fetchHypotheticalIndexes(ctx)

	// Verify the error was properly returned
	require.Error(t, err)
	require.Contains(t, err.Error(), "database error")
}

func TestFetchHypotheticalIndexesEmpty(t *testing.T) {
	ctx := context.Background()

	// Create mock executor that returns empty results
	mockExecutor := &MockSQLExecutor{
		QueryResults: map[string][]tree.Datums{
			// Match any query with empty results
			"": {},
		},
	}

	// Create the explainer with our mock
	explainer := NewHypoIndexExplainer(nil, mockExecutor)

	// Call the function we want to test
	indexes, err := explainer.fetchHypotheticalIndexes(ctx)

	// Verify results - should get empty slice, not nil
	require.NoError(t, err)
	require.Equal(t, 0, len(indexes))
	require.NotNil(t, indexes, "Should return empty slice, not nil")
}
