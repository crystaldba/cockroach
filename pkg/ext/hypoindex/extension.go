// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hypoindex

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
)

// RegisterExtension would register the hypoindex extension with CockroachDB.
func RegisterExtension() {
	// This would be implemented with the appropriate CockroachDB API
}

// CreateExtensionHook is called when the extension is created.
func CreateExtensionHook(ctx context.Context, p interface{}) error {
	// Create the tables and functions for the extension
	return registerHypoindexTablesAndFunctions(ctx, p)
}

// registerHypoindexTablesAndFunctions creates the tables and functions needed for the extension.
func registerHypoindexTablesAndFunctions(ctx context.Context, p interface{}) error {
	// Example of SQL that would be created in a real implementation
	_ = fmt.Sprintf(`
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

	// In a real implementation, this SQL would be executed with:
	// if err := p.ExecuteSQL(ctx, tableSQL); err != nil {
	//     return err
	// }

	// Skipping the implementation of additional SQL statements
	// and function registration for brevity

	return nil
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
