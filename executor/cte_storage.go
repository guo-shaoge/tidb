// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"hash"
	"hash/fnv"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/memory"
)

var (
	_ CTEStorage = &CTEStorageRC{}
)

// CTEStorage is a temporary storage to store the intermidate data of CTE.
//
// Common usage as follows:
//
//  storage.Lock()
//  if !storage.Done() {
//      fill all data into storage
//  }
//  storage.UnLock()
//  read data from storage
// TODO: make function order be ok
type CTEStorage interface {
	// If is first called, will open underlying storage. Otherwise will add ref count by one
	OpenAndRef(fieldType []*types.FieldType, chkSize int) error

	// Minus ref count by one, if ref count is zero, close underlying storage
	DerefAndClose() (err error)

	// Swap data of two storage. Other metainfo is not touched, such ref count/done flag etc
	SwapData(other CTEStorage) error

	// Data will first resides in RAM, when exceeds limit, will spill all data to disk automatically
	// Memory limit is control by memTracker.
	Add(chk *chunk.Chunk) error

	GetChunk(chkIdx int) (*chunk.Chunk, error)

	// CTEStorage is not thread-safe. By Lock(), users can achieve the purpose of ensuring thread safety
	Lock()
	Unlock()

	// Usually, CTEStorage is filled first, then user can read it.
	// User can check whether CTEStorage is filled first, if not, they can fill it.
	Done() bool
	SetDone()

	ResetData() error
	NumChunks() int

	// TODO: is this ok?
	GetMemTracker() *memory.Tracker
	GetDiskTracker() *disk.Tracker
	ActionSpill() memory.ActionOnExceed

	SetIter(iter int)
	GetIter() int
}

// CTEStorage implementation using RowContainer
type CTEStorageRC struct {
	// meta info
	mu        sync.Mutex
	refCnt    int
	done      bool
	iter      int
	filterDup bool
	sc        *stmtctx.StatementContext

	// data info
	tp []*types.FieldType
	rc *chunk.RowContainer
	// TODO: memtrack ht
	ht baseHashTable
}

func NewCTEStorageRC(sc *stmtctx.StatementContext, filterDup bool) *CTEStorageRC {
	return &CTEStorageRC{sc: sc, filterDup: filterDup}
}

// OpenAndRef impl CTEStorage OpenAndRef interface
func (s *CTEStorageRC) OpenAndRef(fieldType []*types.FieldType, chkSize int) (err error) {
	if !s.valid() {
		s.tp = fieldType
		s.rc = chunk.NewRowContainer(fieldType, chkSize)
		s.refCnt = 1
		if s.filterDup {
			s.ht = newConcurrentMapHashTable()
		}
	} else {
		s.refCnt += 1
	}
	return nil
}

// DerefAndClose impl CTEStorage DerefAndClose interface
func (s *CTEStorageRC) DerefAndClose() (err error) {
	if !s.valid() {
		return errors.Trace(errors.New("CTEStorage not opend yet"))
	}
	s.refCnt -= 1
	if s.refCnt == 0 {
		if err = s.rc.Close(); err != nil {
			return err
		}
		if err = s.resetAll(); err != nil {
			return err
		}
	}
	return nil
}

// Swap impl CTEStorage Swap interface
func (s *CTEStorageRC) SwapData(other CTEStorage) (err error) {
	otherRC, ok := other.(*CTEStorageRC)
	if !ok {
		return errors.Trace(errors.New("cannot swap if underlying storages are different"))
	}
	if s.filterDup != otherRC.filterDup {
		return errors.Trace(errors.New("cannot swap if filterDup flags are different"))
	}
	s.rc, otherRC.rc = otherRC.rc, s.rc
	s.tp, otherRC.tp = otherRC.tp, s.tp
	s.ht, otherRC.ht = otherRC.ht, s.ht
	return nil
}

// Add impl CTEStorage Add interface
func (s *CTEStorageRC) Add(chk *chunk.Chunk) (err error) {
	if !s.valid() {
		return errors.Trace(errors.New("CTEStorage is not valid"))
	}
	if s.filterDup {
		if chk, err = s.filterAndAddHashTable(s.sc, chk); err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			return nil
		}
	}
	return s.rc.Add(chk)
}

// GetChunk impl CTEStorage GetChunk interface
func (s *CTEStorageRC) GetChunk(chkIdx int) (*chunk.Chunk, error) {
	if !s.valid() {
		return nil, errors.Trace(errors.New("CTEStorage is not valid"))
	}
	return s.rc.GetChunk(chkIdx)
}

func (s *CTEStorageRC) Lock() {
	s.mu.Lock()
}

func (s *CTEStorageRC) Unlock() {
	s.mu.Unlock()
}

func (s *CTEStorageRC) Done() bool {
	return s.done
}

func (s *CTEStorageRC) SetDone() {
	s.done = true
}

func (s *CTEStorageRC) ResetData() error {
	return s.rc.Reset()
}

func (s *CTEStorageRC) NumChunks() int {
	return s.rc.NumChunks()
}

func (s *CTEStorageRC) GetMemTracker() *memory.Tracker {
	return s.rc.GetMemTracker()
}

func (s *CTEStorageRC) GetDiskTracker() *memory.Tracker {
	return s.rc.GetDiskTracker()
}

func (s *CTEStorageRC) ActionSpill() memory.ActionOnExceed {
	return s.rc.ActionSpill()
}

func (s *CTEStorageRC) SetIter(iter int) {
	s.iter = iter
}

func (s *CTEStorageRC) GetIter() int {
	return s.iter
}

// Swap data and metadata. not used for now.
func (s *CTEStorageRC) swapAll(other *CTEStorageRC) (err error) {
	s.refCnt, other.refCnt = other.refCnt, s.refCnt
	s.rc, other.rc = other.rc, s.rc
	s.mu, other.mu = other.mu, s.mu
	s.done, other.done = other.done, s.done
	s.iter, other.iter = other.iter, s.iter
	s.sc, other.sc = other.sc, s.sc
	return nil
}

func (s *CTEStorageRC) resetAll() error {
	s.refCnt = -1
	s.done = false
	s.iter = 0
	s.filterDup = false
	s.sc = nil
	// s.ht has no reset() method
	return s.rc.Reset()
}

func (s *CTEStorageRC) valid() bool {
	return s.refCnt > 0 && s.rc != nil
}

func (s *CTEStorageRC) filterAndAddHashTable(sc *stmtctx.StatementContext, chk *chunk.Chunk) (chkNoDup *chunk.Chunk, err error) {
	rows := chk.NumRows()

	buf := make([]byte, 1)
	isNull := make([]bool, rows)
	hasher := make([]hash.Hash64, rows)
	for i := 0; i < rows; i++ {
		// TODO: need reset every time?
		hasher[i] = fnv.New64()
	}

	for i := 0; i < chk.NumCols(); i++ {
		err = codec.HashChunkColumns(sc, hasher, chk, s.tp[i], i, buf, isNull)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	chkNoDup = chunk.NewChunkWithCapacity(s.tp, chk.Capacity())

	chkIdx := s.rc.NumChunks()
	for i := 0; i < rows; i++ {
		key := hasher[i].Sum64()
		row := chk.GetRow(i)

		hasDup, err := s.checkHasDup(key, row)
		if err != nil {
			return nil, err
		}
		if hasDup {
			continue
		}

		chkNoDup.AppendRow(row)

		rowPtr := chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(i)}
		s.ht.Put(key, rowPtr)
	}
	return chkNoDup, nil
}

func (s *CTEStorageRC) checkHasDup(probeKey uint64, row chunk.Row) (bool, error) {
	ptrs := s.ht.Get(probeKey)

	colIdx := make([]int, row.Len())
	for i := range colIdx {
		colIdx[i] = i
	}

	for _, ptr := range ptrs {
		matchedRow, err := s.rc.GetRow(ptr)
		if err != nil {
			return false, err
		}
		isEqual, err := codec.EqualChunkRow(s.sc, row, s.tp, colIdx, matchedRow, s.tp, colIdx)
		if err != nil {
			return false, err
		}
		if isEqual {
			return true, nil
		}
	}
	return false, nil
}
