// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// This file contains a simple test function that can be called
// to manually verify the hypo_index_explain function.
// It's not meant to be run as part of the automated tests.

// testHypoIndexExplain is a simple manual test for the hypo_index_explain function.
// To use it, add code to call it from a relevant place in the codebase.
func testHypoIndexExplain() {
	// Test data
	query := "SELECT * FROM test_table WHERE a = 1"
	indexStmt := "CREATE INDEX idx_a ON test_table (a)"

	// Parse the index statement to verify it's valid
	idxStmts, err := parser.Parse(indexStmt)
	if err != nil {
		fmt.Printf("Error parsing index statement: %v\n", err)
		return
	}

	if len(idxStmts) != 1 {
		fmt.Println("Expected exactly one statement")
		return
	}

	// Verify that this is actually a CREATE INDEX statement
	_, ok := idxStmts[0].AST.(*tree.CreateIndex)
	if !ok {
		fmt.Println("Statement is not a CREATE INDEX statement")
		return
	}

	fmt.Printf("Successfully parsed query: %s\n", query)
	fmt.Printf("Successfully parsed index statement: %s\n", indexStmt)
	fmt.Println("Manual test successful!")
}

// To run the test, add the following to some reachable code:
//
// func someFunction() {
//   testHypoIndexExplain()
// }
