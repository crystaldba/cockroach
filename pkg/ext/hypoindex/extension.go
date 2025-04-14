// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hypoindex

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ExtensionExecutor represents an interface for executing SQL in the extension context.
type ExtensionExecutor interface {
	ExecuteSQL(ctx context.Context, sql string) error
	RegisterFunction(name string, fn interface{}) error
}

// RegisterExtension would register the hypoindex extension with CockroachDB.
func RegisterExtension() {
	// This would be implemented with the appropriate CockroachDB API
}

// CreateExtensionHook is called when the extension is created.
func CreateExtensionHook(ctx context.Context, p interface{}) error {
	// If p implements our expected interface, use it
	if executor, ok := p.(ExtensionExecutor); ok {
		return registerHypoindexTablesAndFunctions(ctx, executor)
	}
	// Otherwise, just return success
	return nil
}

// registerHypoindexTablesAndFunctions creates the tables and functions needed for the extension.
func registerHypoindexTablesAndFunctions(ctx context.Context, executor ExtensionExecutor) error {
	// Create the hypo_indexes table
	tableSQL := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.hypo_indexes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  table_schema STRING NOT NULL,
  table_name STRING NOT NULL,
  index_name STRING NOT NULL,
  columns STRING[] NOT NULL,
  storing STRING[] NULL,
  unique BOOL NOT NULL DEFAULT FALSE,
  inverted BOOL NOT NULL DEFAULT FALSE,
  visible BOOL NOT NULL DEFAULT TRUE,
  predicate STRING NULL
)`, catconstants.PgExtensionSchemaName)

	if err := executor.ExecuteSQL(ctx, tableSQL); err != nil {
		return fmt.Errorf("error creating hypo_indexes table: %w", err)
	}

	// Create function to add a hypothetical index
	createIndexSQL := fmt.Sprintf(`
CREATE FUNCTION %s.hypo_create_index(
  table_schema STRING,
  table_name STRING, 
  index_name STRING, 
  columns STRING[],
  storing STRING[] DEFAULT NULL,
  unique BOOL DEFAULT FALSE,
  inverted BOOL DEFAULT FALSE
) RETURNS UUID AS $$
DECLARE
  idx_id UUID;
BEGIN
  INSERT INTO %s.hypo_indexes (
    table_schema, table_name, index_name, columns, storing, unique, inverted
  ) VALUES (
    table_schema, table_name, index_name, columns, storing, unique, inverted
  ) RETURNING id INTO idx_id;
  RETURN idx_id;
END;
$$ LANGUAGE plpgsql;`, catconstants.PgExtensionSchemaName, catconstants.PgExtensionSchemaName)

	if err := executor.ExecuteSQL(ctx, createIndexSQL); err != nil {
		return fmt.Errorf("error creating hypo_create_index function: %w", err)
	}

	// Create function to drop a hypothetical index
	dropIndexSQL := fmt.Sprintf(`
CREATE FUNCTION %s.hypo_drop_index(index_id UUID) RETURNS BOOL AS $$
BEGIN
  DELETE FROM %s.hypo_indexes WHERE id = index_id;
  RETURN FOUND;
END;
$$ LANGUAGE plpgsql;`, catconstants.PgExtensionSchemaName, catconstants.PgExtensionSchemaName)

	if err := executor.ExecuteSQL(ctx, dropIndexSQL); err != nil {
		return fmt.Errorf("error creating hypo_drop_index function: %w", err)
	}

	// Create function to drop all hypothetical indexes
	dropAllSQL := fmt.Sprintf(`
CREATE FUNCTION %s.hypo_drop_all_indexes() RETURNS INT AS $$
DECLARE
  count INT;
BEGIN
  DELETE FROM %s.hypo_indexes RETURNING count(*) INTO count;
  RETURN count;
END;
$$ LANGUAGE plpgsql;`, catconstants.PgExtensionSchemaName, catconstants.PgExtensionSchemaName)

	if err := executor.ExecuteSQL(ctx, dropAllSQL); err != nil {
		return fmt.Errorf("error creating hypo_drop_all_indexes function: %w", err)
	}

	// Create function to list hypothetical indexes
	listIndexesSQL := fmt.Sprintf(`
CREATE FUNCTION %s.hypo_list_indexes() RETURNS TABLE (
  id UUID,
  table_schema STRING,
  table_name STRING,
  index_name STRING,
  columns STRING[],
  storing STRING[],
  unique BOOL,
  inverted BOOL,
  visible BOOL,
  predicate STRING
) AS $$
  SELECT id, table_schema, table_name, index_name, columns, storing, unique, inverted, visible, predicate
  FROM %s.hypo_indexes;
$$ LANGUAGE sql;`, catconstants.PgExtensionSchemaName, catconstants.PgExtensionSchemaName)

	if err := executor.ExecuteSQL(ctx, listIndexesSQL); err != nil {
		return fmt.Errorf("error creating hypo_list_indexes function: %w", err)
	}

	// Register the explain function - this one is implemented in Go
	if err := executor.RegisterFunction("hypo_explain", hypoExplainFunc); err != nil {
		return fmt.Errorf("error registering hypo_explain function: %w", err)
	}

	return nil
}

// hypoExplainFunc is the SQL function implementation for hypo_explain
var hypoExplainFunc = func(ctx context.Context, evalCtx interface{}, args tree.Datums) (tree.Datum, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("hypo_explain requires exactly one argument")
	}

	queryString, ok := tree.AsDString(args[0])
	if !ok {
		return nil, fmt.Errorf("hypo_explain requires a string argument")
	}

	// Create an explainer and run the query
	explainer := NewHypoIndexExplainer(evalCtx)
	result, err := explainer.ExplainQuery(ctx, string(queryString))
	if err != nil {
		return nil, err
	}

	return tree.NewDString(result), nil
}

// HypoExplainImpl is the implementation of the hypo_explain function for testing.
func HypoExplainImpl(ctx context.Context, args []interface{}) (interface{}, error) {
	// Get the query string from the first argument
	queryString, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("expected string argument, got %T", args[0])
	}

	// Create an explainer and run the query
	explainer := NewHypoIndexExplainer(nil)
	result, err := explainer.ExplainQuery(ctx, queryString)
	if err != nil {
		return nil, err
	}

	return result, nil
}
