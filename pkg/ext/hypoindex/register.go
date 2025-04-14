// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hypoindex

// This file will be used to register the extension with CockroachDB's
// extension system once the proper API is available.

// init registers the hypoindex extension with CockroachDB.
func init() {
	// This is a placeholder registration that would be replaced
	// with actual registration code once the extension system
	// provides a proper API.
	//
	// Example:
	// sql.RegisterExtension("hypoindex", CreateExtensionHook)
}

// RegisterBuiltinFunctions registers any builtin functions provided by this
// extension. This would be called during server startup.
func RegisterBuiltinFunctions() {
	// Register any builtin functions provided by the extension
	// Example:
	// sql.RegisterBuiltin(tree.FunctionDefinition{
	//     Name:       "hypo_explain",
	//     Definition: hypoExplainFnImpl,
	// })
}
