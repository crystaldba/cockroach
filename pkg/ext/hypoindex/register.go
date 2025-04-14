// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hypoindex

// This file contains registration logic for the hypoindex extension.
// It would be expanded when CockroachDB provides a complete extension API.

// init registers the hypoindex extension with CockroachDB.
func init() {
	// This demonstrates the expected registration pattern:
	//
	// RegisterExtension("hypoindex", ExtensionInfo{
	//   Description: "Hypothetical indexes for EXPLAIN",
	//   Version:     "1.0",
	//   OnCreate:    CreateExtensionHook,
	//   OnDrop:      DropExtensionHook,
	// })
}

// BuiltinInfo defines information about a builtin function for the extension.
type BuiltinInfo struct {
	Name       string
	Category   string
	Arguments  []string
	ReturnType string
	Function   interface{}
}

// RegisterBuiltinFunctions registers any builtin functions provided by this
// extension. This would be called during server startup.
func RegisterBuiltinFunctions() []BuiltinInfo {
	// Return the list of builtin functions that should be registered
	return []BuiltinInfo{
		{
			Name:       "hypo_explain",
			Category:   "Hypothetical Indexes",
			Arguments:  []string{"query STRING"},
			ReturnType: "STRING",
			Function:   hypoExplainFunc,
		},
	}
}

// DropExtensionHook is called when the extension is dropped.
func DropExtensionHook(ctx interface{}, p interface{}) error {
	// In a real implementation, this would:
	// 1. Drop the hypo_indexes table
	// 2. Drop all the extension functions
	// 3. Clean up any other resources

	return nil
}
