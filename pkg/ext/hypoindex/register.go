// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hypoindex

// This file will be used to register the extension with CockroachDB's
// extension system once the proper API is available.

// init registers the hypoindex extension with CockroachDB.
func init() {
	// Register our extension with CockroachDB's extension system.
	// The actual registration happens in pkg/sql/create_extension.go
	// sql.RegisterExtension(sql.CreateExtensionDefinition{
	// 	Name:       "hypoindex",
	// 	OnCreate:   CreateExtensionHook,
	// 	OnActivate: ActivateExtensionHook,
	// })
}

// RegisterBuiltinFunctions registers any builtin functions provided by this
// extension. This would be called during server startup.
func RegisterBuiltinFunctions() {
	// Register any builtin functions provided by the extension
	// Example:
	// sql.RegisterBuiltin("hypo_explain", hypoExplainImpl)
}
