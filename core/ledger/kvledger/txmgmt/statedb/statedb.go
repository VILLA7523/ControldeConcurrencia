/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statedb

import (
	"math"
	"sort"

	"github.com/hyperledger/fabric/core/ledger/internal/version"
	"github.com/hyperledger/fabric/core/ledger/util"
)

//go:generate counterfeiter -o mock/results_iterator.go -fake-name ResultsIterator . ResultsIterator
//go:generate counterfeiter -o mock/versioned_db.go -fake-name VersionedDB . VersionedDB
//go:generate counterfeiter -o mock/namespace_provider.go -fake-name NamespaceProvider . NamespaceProvider

// VersionedDBProvider provides an instance of an versioned DB
type VersionedDBProvider interface {
	// GetDBHandle returns a handle to a VersionedDB
	GetDBHandle(id string, namespaceProvider NamespaceProvider) (VersionedDB, error)
	// Close closes all the VersionedDB instances and releases any resources held by VersionedDBProvider
	Close()
}

// VersionedDB lists methods that a db is supposed to implement
type VersionedDB interface {
	GetSnapshotState(snapshot uint64, ns string, key string) (*VersionedValue, error)
	// GetState gets the value for given namespace and key. For a chaincode, the namespace corresponds to the chaincodeId
	GetState(namespace string, key string) (*VersionedValue, error)
	// GetVersion gets the version for given namespace and key. For a chaincode, the namespace corresponds to the chaincodeId
	GetVersion(namespace string, key string) (*version.Height, error)
	// GetStateMultipleKeys gets the values for multiple keys in a single call
	GetStateMultipleKeys(namespace string, keys []string) ([]*VersionedValue, error)
	// GetStateRangeScanIterator returns an iterator that contains all the key-values between given key ranges.
	// startKey is inclusive
	// endKey is exclusive
	// The returned ResultsIterator contains results of type *VersionedKV
	GetStateRangeScanIterator(namespace string, startKey string, endKey string) (ResultsIterator, error)
	// GetStateRangeScanIteratorWithPagination returns an iterator that contains all the key-values between given key ranges.
	// startKey is inclusive
	// endKey is exclusive
	// pageSize parameter limits the number of returned results
	// The returned ResultsIterator contains results of type *VersionedKV
	GetStateRangeScanIteratorWithPagination(namespace string, startKey string, endKey string, pageSize int32) (QueryResultsIterator, error)
	// ExecuteQuery executes the given query and returns an iterator that contains results of type *VersionedKV.
	ExecuteQuery(namespace, query string) (ResultsIterator, error)
	// ExecuteQueryWithPagination executes the given query and
	// returns an iterator that contains results of type *VersionedKV.
	// The bookmark and page size parameters are associated with the pagination query.
	ExecuteQueryWithPagination(namespace, query, bookmark string, pageSize int32) (QueryResultsIterator, error)
	// ApplyUpdates applies the batch to the underlying db.
	// height is the height of the highest transaction in the Batch that
	// a state db implementation is expected to ues as a save point
	ApplyUpdates(batch *UpdateBatch, height *version.Height) error
	// GetLatestSavePoint returns the height of the highest transaction upto which
	// the state db is consistent
	GetLatestSavePoint() (*version.Height, error)
	// ValidateKeyValue tests whether the key and value is supported by the db implementation.
	// For instance, leveldb supports any bytes for the key while the couchdb supports only valid utf-8 string
	// TODO make the function ValidateKeyValue return a specific error say ErrInvalidKeyValue
	// However, as of now, the both implementations of this function (leveldb and couchdb) are deterministic in returing an error
	// i.e., an error is returned only if the key-value are found to be invalid for the underlying db
	ValidateKeyValue(key string, value []byte) error
	// BytesKeySupported returns true if the implementation (underlying db) supports the any bytes to be used as key.
	// For instance, leveldb supports any bytes for the key while the couchdb supports only valid utf-8 string
	BytesKeySupported() bool
	// GetFullScanIterator returns a FullScanIterator that can be used to iterate over entire data in the statedb.
	// `skipNamespace` parameter can be used to control if the consumer wants the FullScanIterator
	// to skip one or more namespaces from the returned results. The second parameter returns the format information
	// about the value bytes returned by the Next function in the returned FullScanIterator.
	// The intended use of this iterator is to generate the snapshot files for the statedb.
	GetFullScanIterator(skipNamespace func(string) bool) (FullScanIterator, byte, error)
	// Open opens the db
	Open() error
	// Close closes the db
	Close()
}

// NamespaceProvider provides a mean for statedb to get all the possible namespaces for a channel.
// The intended use is for statecouchdb to retroactively build channel metadata when it is missing,
// e.g., when opening a statecouchdb from v2.0/2.1 version.
type NamespaceProvider interface {
	// PossibleNamespaces returns all possible namespaces for the statedb. Note that it is a superset
	// of the actual namespaces. Therefore, the caller should compare with the existing databases to
	// filter out the namespaces that have no matched databases.
	PossibleNamespaces(vdb VersionedDB) ([]string, error)
}

//BulkOptimizable interface provides additional functions for
//databases capable of batch operations
type BulkOptimizable interface {
	LoadCommittedVersions(keys []*CompositeKey) error
	GetCachedVersion(namespace, key string) (*version.Height, bool)
	ClearCachedVersions()
}

//IndexCapable interface provides additional functions for
//databases capable of index operations
type IndexCapable interface {
	GetDBType() string
	ProcessIndexesForChaincodeDeploy(namespace string, indexFilesData map[string][]byte) error
}

// FullScanIterator provides a mean to iterate over entire statedb. The intended use of this iterator
// is to generate the snapshot files for the statedb
type FullScanIterator interface {
	// Next returns the key-values in the lexical order of <Namespace, key>
	// A particular statedb implementation is free to chose any deterministic bytes representation for the <version, value, metadata>
	Next() (*CompositeKey, []byte, error)
	// Close releases any resources held with the implementation
	Close()
}

// CompositeKey encloses Namespace and Key components
type CompositeKey struct {
	Namespace string
	Key       string
}

// VersionedValue encloses value and corresponding version
type VersionedValue struct {
	Value    []byte
	Metadata []byte
	Version  *version.Height
}

// IsDelete returns true if this update indicates delete of a key
func (vv *VersionedValue) IsDelete() bool {
	return vv.Value == nil
}

// VersionedKV encloses key and corresponding VersionedValue
type VersionedKV struct {
	CompositeKey
	VersionedValue
}

// ResultsIterator iterates over query results
type ResultsIterator interface {
	Next() (QueryResult, error)
	Close()
}

// QueryResultsIterator adds GetBookmarkAndClose method
type QueryResultsIterator interface {
	ResultsIterator
	GetBookmarkAndClose() string
}

// QueryResult - a general interface for supporting different types of query results. Actual types differ for different queries
type QueryResult interface{}

type nsUpdates struct {
	M            map[string]*VersionedValue
	TxnIDs       map[string]string
	Deps         map[string][]string
	DepSnapshots map[string]uint64
}

func newNsUpdates() *nsUpdates {
	return &nsUpdates{make(map[string]*VersionedValue),
		make(map[string]string),
		make(map[string][]string),
		make(map[string]uint64),
	}
}

// UpdateBatch encloses the details of multiple `updates`
type UpdateBatch struct {
	ContainsPostOrderWrites bool
	Updates                 map[string]*nsUpdates
}

// NewUpdateBatch constructs an instance of a Batch
func NewUpdateBatch() *UpdateBatch {
	return &UpdateBatch{false, make(map[string]*nsUpdates)}
}

// Get returns the VersionedValue for the given namespace and key
func (batch *UpdateBatch) Get(ns string, key string) *VersionedValue {
	nsUpdates, ok := batch.Updates[ns]
	if !ok {
		return nil
	}
	vv, ok := nsUpdates.M[key]
	if !ok {
		return nil
	}
	return vv
}

// Put adds a key with value only. The metadata is assumed to be nil
func (batch *UpdateBatch) Put(ns string, key string, value []byte, version *version.Height) {
	batch.PutValAndMetadata(ns, key, value, nil, version)
}

func (batch *UpdateBatch) PutValTxnIdDep(ns string, key string, value []byte, version *version.Height, txnID string, deps []string, depSnapshot uint64) {
	batch.PutValAndMetadataTxnIdDeps(ns, key, value, nil, version, txnID, deps, depSnapshot)
}

// PutValAndMetadata adds a key with value and metadata
// TODO introducing a new function to limit the refactoring. Later in a separate CR, the 'Put' function above should be removed
func (batch *UpdateBatch) PutValAndMetadata(ns string, key string, value []byte, metadata []byte, version *version.Height) {
	if value == nil {
		panic("Nil value not allowed. Instead call 'Delete' function")
	}
	batch.Update(ns, key, &VersionedValue{value, metadata, version})
}

func (batch *UpdateBatch) PutValAndMetadataTxnIdDeps(ns string, key string, value []byte, metadata []byte, version *version.Height, txnID string, deps []string, depSnapshot uint64) {
	if value == nil {
		panic("Nil value not allowed. Instead call 'Delete' function")
	}
	batch.UpdateValTxnIdDeps(ns, key, &VersionedValue{value, metadata, version}, txnID, deps, depSnapshot)
}

// Delete deletes a Key and associated value
func (batch *UpdateBatch) Delete(ns string, key string, version *version.Height) {
	batch.Update(ns, key, &VersionedValue{nil, nil, version})
}

func (batch *UpdateBatch) DeleteWithTxnIdDeps(ns string, key string, version *version.Height, txnID string, deps []string, depSnapshot uint64) {
	batch.UpdateValTxnIdDeps(ns, key, &VersionedValue{nil, nil, version}, txnID, deps, depSnapshot)
}

// Exists checks whether the given key exists in the batch
func (batch *UpdateBatch) Exists(ns string, key string) bool {
	nsUpdates, ok := batch.Updates[ns]
	if !ok {
		return false
	}
	_, ok = nsUpdates.M[key]
	return ok
}

// GetUpdatedNamespaces returns the names of the namespaces that are updated
func (batch *UpdateBatch) GetUpdatedNamespaces() []string {
	namespaces := make([]string, len(batch.Updates))
	i := 0
	for ns := range batch.Updates {
		namespaces[i] = ns
		i++
	}
	return namespaces
}

// Update updates the batch with a latest entry for a namespace and a key
func (batch *UpdateBatch) Update(ns string, key string, vv *VersionedValue) {
	batch.getOrCreateNsUpdates(ns).M[key] = vv
}

func (batch *UpdateBatch) UpdateValTxnIdDeps(ns string, key string, vv *VersionedValue, txnID string, dep []string, depSnapshot uint64) {
	batch.getOrCreateNsUpdates(ns).M[key] = vv
	batch.getOrCreateNsUpdates(ns).TxnIDs[key] = txnID
	batch.getOrCreateNsUpdates(ns).Deps[key] = dep
	batch.getOrCreateNsUpdates(ns).DepSnapshots[key] = depSnapshot
}

// GetUpdates returns all the updates for a namespace
func (batch *UpdateBatch) GetUpdates(ns string) map[string]*VersionedValue {
	nsUpdates, ok := batch.Updates[ns]
	if !ok {
		return nil
	}
	return nsUpdates.M
}

func (batch *UpdateBatch) GetTxnIds(ns string) map[string]string {
	nsUpdates, ok := batch.Updates[ns]
	if !ok {
		return nil
	}
	return nsUpdates.TxnIDs
}

func (batch *UpdateBatch) GetDeps(ns string) map[string][]string {
	nsUpdates, ok := batch.Updates[ns]
	if !ok {
		return nil
	}
	return nsUpdates.Deps
}

func (batch *UpdateBatch) GetDepSnapshots(ns string) map[string]uint64 {
	nsUpdates, ok := batch.Updates[ns]
	if !ok {
		return nil
	}
	return nsUpdates.DepSnapshots
}

// GetRangeScanIterator returns an iterator that iterates over keys of a specific namespace in sorted order
// In other word this gives the same functionality over the contents in the `UpdateBatch` as
// `VersionedDB.GetStateRangeScanIterator()` method gives over the contents in the statedb
// This function can be used for querying the contents in the updateBatch before they are committed to the statedb.
// For instance, a validator implementation can used this to verify the validity of a range query of a transaction
// where the UpdateBatch represents the union of the modifications performed by the preceding valid transactions in the same block
// (Assuming Group commit approach where we commit all the updates caused by a block together).
func (batch *UpdateBatch) GetRangeScanIterator(ns string, startKey string, endKey string) QueryResultsIterator {
	return newNsIterator(ns, startKey, endKey, batch)
}

// Merge merges another updates batch with this updates batch
func (batch *UpdateBatch) Merge(toMerge *UpdateBatch) {
	batch.ContainsPostOrderWrites = batch.ContainsPostOrderWrites || toMerge.ContainsPostOrderWrites
	for ns, nsUpdates := range toMerge.Updates {
		for key, vv := range nsUpdates.M {
			// Each update must accompany its txnID
			txnID := "fake" // this value can NOT be "" due to the ustore requirement
			if t, ok := nsUpdates.TxnIDs[key]; ok {
				txnID = t
			}
			var deps []string
			if d, ok := nsUpdates.Deps[key]; ok {
				deps = d
			}

			var depSnapshot uint64 = math.MaxUint64
			if d, ok := nsUpdates.DepSnapshots[key]; ok {
				depSnapshot = d
			}

			batch.UpdateValTxnIdDeps(ns, key, vv, txnID, deps, depSnapshot)
		}
	}
}

func (batch *UpdateBatch) getOrCreateNsUpdates(ns string) *nsUpdates {
	nsUpdates := batch.Updates[ns]
	if nsUpdates == nil {
		nsUpdates = newNsUpdates()
		batch.Updates[ns] = nsUpdates
	}
	return nsUpdates
}

type nsIterator struct {
	ns         string
	nsUpdates  *nsUpdates
	sortedKeys []string
	nextIndex  int
	lastIndex  int
}

func newNsIterator(ns string, startKey string, endKey string, batch *UpdateBatch) *nsIterator {
	nsUpdates, ok := batch.Updates[ns]
	if !ok {
		return &nsIterator{}
	}
	sortedKeys := util.GetSortedKeys(nsUpdates.M)
	var nextIndex int
	var lastIndex int
	if startKey == "" {
		nextIndex = 0
	} else {
		nextIndex = sort.SearchStrings(sortedKeys, startKey)
	}
	if endKey == "" {
		lastIndex = len(sortedKeys)
	} else {
		lastIndex = sort.SearchStrings(sortedKeys, endKey)
	}
	return &nsIterator{ns, nsUpdates, sortedKeys, nextIndex, lastIndex}
}

// Next gives next key and versioned value. It returns a nil when exhausted
func (itr *nsIterator) Next() (QueryResult, error) {
	if itr.nextIndex >= itr.lastIndex {
		return nil, nil
	}
	key := itr.sortedKeys[itr.nextIndex]
	vv := itr.nsUpdates.M[key]
	itr.nextIndex++
	return &VersionedKV{CompositeKey{itr.ns, key}, VersionedValue{vv.Value, vv.Metadata, vv.Version}}, nil
}

// Close implements the method from QueryResult interface
func (itr *nsIterator) Close() {
	// do nothing
}

// GetBookmarkAndClose implements the method from QueryResult interface
func (itr *nsIterator) GetBookmarkAndClose() string {
	// do nothing
	return ""
}

// These three structs must have the identical structure with those
//   in https://github.com/RUAN0007/fabric-chaincode-go/blob/master/shim/interfaces.go
type HistResult struct {
	Msg        string
	Val        string
	CreatedBlk uint64
}

type BackwardResult struct {
	Msg       string
	DepKeys   []string
	DepBlkIdx []uint64
	TxnID     string
}

type ForwardResult struct {
	Msg           string
	ForwardKeys   []string
	ForwardBlkIdx []uint64
	ForwardTxnIDs []string
}
