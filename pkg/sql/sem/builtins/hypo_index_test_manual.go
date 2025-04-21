// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build ignore
// +build ignore

package main

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func main() {
	// Create a test context
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)

	// Test data
	testQuery := "SELECT * FROM test_table WHERE a = 1"
	testIndexes := []string{
		"CREATE INDEX idx_a ON test_table (a)",
	}

	// Prepare inputs
	args := make(tree.Datums, 0, 3)
	args = append(args, tree.NewDString(testQuery))

	// Create an array for the indexes
	indexesArray := tree.NewDArray(types.String)
	for _, idx := range testIndexes {
		if err := indexesArray.Append(tree.NewDString(idx)); err != nil {
			fmt.Printf("Error appending to array: %v\n", err)
			return
		}
	}
	args = append(args, indexesArray)

	// Add VERBOSE option
	args = append(args, tree.NewDString("VERBOSE"))

	// Call the function directly
	result, err := builtins.HypoIndexExplain(ctx, &evalCtx, args)
	if err != nil {
		fmt.Printf("Error executing hypo_index_explain: %v\n", err)
		return
	}

	fmt.Println("Result:")
	fmt.Println(string(tree.MustBeDString(result)))

	// Parse query and index statements to verify they parse correctly
	_, err = parser.ParseOne(testQuery)
	if err != nil {
		fmt.Printf("Error parsing query: %v\n", err)
		return
	}

	for _, idx := range testIndexes {
		stmts, err := parser.Parse(idx)
		if err != nil {
			fmt.Printf("Error parsing CREATE INDEX: %v\n", err)
			return
		}

		// Verify the statement is a CREATE INDEX
		if _, ok := stmts[0].AST.(*tree.CreateIndex); !ok {
			fmt.Printf("Not a CREATE INDEX statement: %s\n", idx)
			return
		}
	}

	fmt.Println("All parsing tests passed.")
}
