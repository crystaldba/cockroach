// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package hypotable provides functionality for creating hypothetical tables
// with hypothetical indexes for query planning and optimization purposes.
package hypotable

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/errors"
)

// HypotheticalIndex represents a hypothetical index for planning purposes.
type HypotheticalIndex struct {
	// TableName is the name of the table containing the index.
	TableName string

	// IndexName is the name of the index.
	IndexName string

	// Columns is the list of columns in the index.
	Columns []string

	// DirectionsBackward indicates whether each column is in descending order.
	DirectionsBackward []bool

	// IsUnique indicates whether the index enforces uniqueness.
	IsUnique bool

	// StoringColumns is the list of columns that are stored but not indexed.
	StoringColumns []string
}

// BuildHypotheticalTableWithIndexes creates a hypothetical table descriptor with
// the original table's schema plus the additional hypothetical indexes.
func BuildHypotheticalTableWithIndexes(
	tableDesc catalog.TableDescriptor, hypoIndexes []HypotheticalIndex,
) (catalog.TableDescriptor, error) {
	// Create a mapping of column names to column IDs
	colNameToID := make(map[string]descpb.ColumnID)
	for _, col := range tableDesc.PublicColumns() {
		colNameToID[col.GetName()] = col.GetID()
	}

	// Verify all columns in the hypothetical indexes exist
	for _, hypoIdx := range hypoIndexes {
		// Verify all indexed columns exist
		if err := verifyColumnsExist(hypoIdx.Columns, colNameToID); err != nil {
			return nil, errors.Wrapf(err, "invalid hypothetical index %q", hypoIdx.IndexName)
		}

		// Verify all storing columns exist
		if err := verifyColumnsExist(hypoIdx.StoringColumns, colNameToID); err != nil {
			return nil, errors.Wrapf(err, "invalid storing columns in hypothetical index %q", hypoIdx.IndexName)
		}
	}

	// Create a new mutable table descriptor copy
	mutableTable := tabledesc.NewBuilder(tableDesc.TableDesc()).BuildCreatedMutable().(*tabledesc.Mutable)

	// Add each hypothetical index to the new table descriptor
	for _, hypoIdx := range hypoIndexes {
		// Convert column names to column IDs for key columns
		var keyColIDs []descpb.ColumnID
		var directions []catenumpb.IndexColumn_Direction
		for i, colName := range hypoIdx.Columns {
			colID := colNameToID[colName]
			keyColIDs = append(keyColIDs, colID)

			direction := catenumpb.IndexColumn_ASC
			if i < len(hypoIdx.DirectionsBackward) && hypoIdx.DirectionsBackward[i] {
				direction = catenumpb.IndexColumn_DESC
			}
			directions = append(directions, direction)
		}

		// Convert column names to column IDs for storing columns
		var storingColIDs []descpb.ColumnID
		for _, colName := range hypoIdx.StoringColumns {
			storingColIDs = append(storingColIDs, colNameToID[colName])
		}

		// Generate a unique ID for the hypothetical index
		idxID := GenerateHypotheticalIndexID(hypoIdx.TableName, hypoIdx.IndexName)

		// Generate a unique name for the hypothetical index
		idxName := fmt.Sprintf("hypo_idx_%s", hypoIdx.IndexName)

		// Create the index descriptor
		idxDesc := descpb.IndexDescriptor{
			ID:                  idxID,
			Name:                idxName,
			KeyColumnIDs:        keyColIDs,
			KeyColumnNames:      hypoIdx.Columns,
			KeyColumnDirections: directions,
			StoreColumnIDs:      storingColIDs,
			StoreColumnNames:    hypoIdx.StoringColumns,
			Unique:              hypoIdx.IsUnique,
			Version:             descpb.SecondaryIndexFamilyFormatVersion,
		}

		// Add the index to the table
		if err := mutableTable.AddSecondaryIndex(idxDesc); err != nil {
			return nil, errors.Wrapf(err, "failed to add hypothetical index %q", hypoIdx.IndexName)
		}
	}

	// Build an immutable table from the mutable one
	return mutableTable.ImmutableCopy().(catalog.TableDescriptor), nil
}

// verifyColumnsExist checks that all column names in the slice exist in the colNameToID map.
func verifyColumnsExist(
	colNames []string, colNameToID map[string]descpb.ColumnID,
) error {
	for _, colName := range colNames {
		if _, ok := colNameToID[colName]; !ok {
			return errors.Newf("column %q does not exist", colName)
		}
	}
	return nil
}

// GenerateHypotheticalIndexID generates a unique ID for a hypothetical index
// based on the table and index name.
func GenerateHypotheticalIndexID(tableName, indexName string) descpb.IndexID {
	// Create a hash of the table and index name
	hash := md5.Sum([]byte(tableName + indexName))

	// Use the first 4 bytes of the hash as a uint32
	id := binary.BigEndian.Uint32(hash[:4])

	// Add a large offset to avoid conflicts with real index IDs
	return descpb.IndexID(id + 10000000)
}
