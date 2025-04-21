// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestHypoIndexExplain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Start a test server
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a SQL runner
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Create a test table
	sqlDB.Exec(t, `
		CREATE TABLE test_table (
			id INT PRIMARY KEY,
			a INT,
			b INT,
			c STRING,
			INDEX (b)
		)
	`)

	// Insert some data
	sqlDB.Exec(t, `
		INSERT INTO test_table (id, a, b, c)
		VALUES (1, 10, 100, 'foo'),
		       (2, 20, 200, 'bar'),
		       (3, 30, 300, 'baz')
	`)

	// Execute the hypo_index_explain function
	var result string
	sqlDB.QueryRow(t, `
		SELECT hypo_index_explain(
			'SELECT * FROM test_table WHERE a > 15',
			ARRAY['CREATE INDEX idx_a ON test_table(a)'],
			'VERBOSE'
		)
	`).Scan(&result)

	// Check that the result contains expected elements
	require.Contains(t, result, "EXPLAIN with hypothetical indexes")
	require.Contains(t, result, "CREATE INDEX ON test_table (a)")

	// Test with multiple hypothetical indexes
	sqlDB.QueryRow(t, `
		SELECT hypo_index_explain(
			'SELECT * FROM test_table WHERE a > 15 AND c LIKE ''b%''',
			ARRAY[
				'CREATE INDEX idx_a ON test_table(a)',
				'CREATE INDEX idx_c ON test_table(c)'
			]
		)
	`).Scan(&result)

	// Check that the result contains expected elements for both indexes
	require.Contains(t, result, "CREATE INDEX ON test_table (a)")
	require.Contains(t, result, "CREATE INDEX ON test_table (c)")

	// Test with an invalid index
	sqlDB.ExpectErr(t, "column \"d\" not found", `
		SELECT hypo_index_explain(
			'SELECT * FROM test_table WHERE a > 15',
			ARRAY['CREATE INDEX idx_d ON test_table(d)']
		)
	`)

	// Test with a nonexistent table
	sqlDB.ExpectErr(t, "failed to resolve table", `
		SELECT hypo_index_explain(
			'SELECT * FROM test_table WHERE a > 15',
			ARRAY['CREATE INDEX idx_a ON nonexistent_table(a)']
		)
	`)
}
