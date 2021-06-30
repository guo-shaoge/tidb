// Copyright 2015 PingCAP, Inc.
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

package structure

import (
	"bytes"
	"context"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
)

// HashPair is the pair for (field, value) in a hash.
type HashPair struct {
	Field []byte
	Value []byte
}

// HSet sets the string value of a hash field.
func (t *TxStructure) HSet(key []byte, field []byte, value []byte) error {
	if t.readWriter == nil {
		return ErrWriteOnSnapshot
	}
	return t.updateHash(key, field, func([]byte) ([]byte, error) {
		return value, nil
	})
}

// HGet gets the value of a hash field.
func (t *TxStructure) HGet(key []byte, field []byte) ([]byte, error) {
	dataKey := t.encodeHashDataKey(key, field)
	value, err := t.reader.Get(context.TODO(), dataKey)
	if kv.ErrNotExist.Equal(err) {
		err = nil
	}
	return value, errors.Trace(err)
}

func (t *TxStructure) hashFieldIntegerVal(val int64) []byte {
	return []byte(strconv.FormatInt(val, 10))
}

// EncodeHashAutoIDKeyValue returns the hash key-value generated by the key and the field
func (t *TxStructure) EncodeHashAutoIDKeyValue(key []byte, field []byte, val int64) (k, v []byte) {
	return t.encodeHashDataKey(key, field), t.hashFieldIntegerVal(val)
}

// HInc increments the integer value of a hash field, by step, returns
// the value after the increment.
func (t *TxStructure) HInc(key []byte, field []byte, step int64) (int64, error) {
	if t.readWriter == nil {
		return 0, ErrWriteOnSnapshot
	}
	base := int64(0)
	err := t.updateHash(key, field, func(oldValue []byte) ([]byte, error) {
		if oldValue != nil {
			var err error
			base, err = strconv.ParseInt(string(oldValue), 10, 64)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		base += step
		return t.hashFieldIntegerVal(base), nil
	})

	return base, errors.Trace(err)
}

// HGetInt64 gets int64 value of a hash field.
func (t *TxStructure) HGetInt64(key []byte, field []byte) (int64, error) {
	value, err := t.HGet(key, field)
	if err != nil || value == nil {
		return 0, errors.Trace(err)
	}

	var n int64
	n, err = strconv.ParseInt(string(value), 10, 64)
	return n, errors.Trace(err)
}

func (t *TxStructure) updateHash(key []byte, field []byte, fn func(oldValue []byte) ([]byte, error)) error {
	dataKey := t.encodeHashDataKey(key, field)
	oldValue, err := t.loadHashValue(dataKey)
	if err != nil {
		return errors.Trace(err)
	}

	newValue, err := fn(oldValue)
	if err != nil {
		return errors.Trace(err)
	}

	// Check if new value is equal to old value.
	if bytes.Equal(oldValue, newValue) {
		return nil
	}

	if err = t.readWriter.Set(dataKey, newValue); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// HDel deletes one or more hash fields.
func (t *TxStructure) HDel(key []byte, fields ...[]byte) error {
	if t.readWriter == nil {
		return ErrWriteOnSnapshot
	}

	for _, field := range fields {
		dataKey := t.encodeHashDataKey(key, field)

		value, err := t.loadHashValue(dataKey)
		if err != nil {
			return errors.Trace(err)
		}

		if value != nil {
			if err = t.readWriter.Delete(dataKey); err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

// HKeys gets all the fields in a hash.
func (t *TxStructure) HKeys(key []byte) ([][]byte, error) {
	var keys [][]byte
	err := t.iterateHash(key, func(field []byte, value []byte) error {
		keys = append(keys, append([]byte{}, field...))
		return nil
	})

	return keys, errors.Trace(err)
}

// HGetAll gets all the fields and values in a hash.
func (t *TxStructure) HGetAll(key []byte) ([]HashPair, error) {
	var res []HashPair
	err := t.iterateHash(key, func(field []byte, value []byte) error {
		pair := HashPair{
			Field: append([]byte{}, field...),
			Value: append([]byte{}, value...),
		}
		res = append(res, pair)
		return nil
	})

	return res, errors.Trace(err)
}

// HGetLastN gets latest N fields and values in hash.
func (t *TxStructure) HGetLastN(key []byte, num int) ([]HashPair, error) {
	res := make([]HashPair, 0, num)
	err := t.iterReverseHash(key, func(field []byte, value []byte) (bool, error) {
		pair := HashPair{
			Field: append([]byte{}, field...),
			Value: append([]byte{}, value...),
		}
		res = append(res, pair)
		if len(res) >= num {
			return false, nil
		}
		return true, nil
	})
	return res, errors.Trace(err)
}

// HClear removes the hash value of the key.
func (t *TxStructure) HClear(key []byte) error {
	err := t.iterateHash(key, func(field []byte, value []byte) error {
		k := t.encodeHashDataKey(key, field)
		return errors.Trace(t.readWriter.Delete(k))
	})

	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (t *TxStructure) iterateHash(key []byte, fn func(k []byte, v []byte) error) error {
	dataPrefix := t.hashDataKeyPrefix(key)
	it, err := t.reader.Iter(dataPrefix, dataPrefix.PrefixNext())
	if err != nil {
		return errors.Trace(err)
	}

	var field []byte

	for it.Valid() {
		if !it.Key().HasPrefix(dataPrefix) {
			break
		}

		_, field, err = t.decodeHashDataKey(it.Key())
		if err != nil {
			return errors.Trace(err)
		}

		if err = fn(field, it.Value()); err != nil {
			return errors.Trace(err)
		}

		err = it.Next()
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// ReverseHashIterator is the reverse hash iterator.
type ReverseHashIterator struct {
	t      *TxStructure
	iter   kv.Iterator
	prefix []byte
	done   bool
	field  []byte
}

// Next implements the Iterator Next.
func (i *ReverseHashIterator) Next() error {
	err := i.iter.Next()
	if err != nil {
		return errors.Trace(err)
	}
	if !i.iter.Key().HasPrefix(i.prefix) {
		i.done = true
		return nil
	}

	_, field, err := i.t.decodeHashDataKey(i.iter.Key())
	if err != nil {
		return errors.Trace(err)
	}
	i.field = field
	return nil
}

// Valid implements the Iterator Valid.
func (i *ReverseHashIterator) Valid() bool {
	return i.iter.Valid() && !i.done
}

// Key implements the Iterator Key.
func (i *ReverseHashIterator) Key() []byte {
	return i.field
}

// Value implements the Iterator Value.
func (i *ReverseHashIterator) Value() []byte {
	return i.iter.Value()
}

// Close Implements the Iterator Close.
func (i *ReverseHashIterator) Close() {
}

// NewHashReverseIter creates a reverse hash iterator.
func NewHashReverseIter(t *TxStructure, key []byte) (*ReverseHashIterator, error) {
	dataPrefix := t.hashDataKeyPrefix(key)
	it, err := t.reader.IterReverse(dataPrefix.PrefixNext())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &ReverseHashIterator{
		t:      t,
		iter:   it,
		prefix: dataPrefix,
	}, nil
}

func (t *TxStructure) iterReverseHash(key []byte, fn func(k []byte, v []byte) (bool, error)) error {
	dataPrefix := t.hashDataKeyPrefix(key)
	it, err := t.reader.IterReverse(dataPrefix.PrefixNext())
	if err != nil {
		return errors.Trace(err)
	}

	var field []byte
	for it.Valid() {
		if !it.Key().HasPrefix(dataPrefix) {
			break
		}

		_, field, err = t.decodeHashDataKey(it.Key())
		if err != nil {
			return errors.Trace(err)
		}

		more, err := fn(field, it.Value())
		if !more || err != nil {
			return errors.Trace(err)
		}

		err = it.Next()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (t *TxStructure) loadHashValue(dataKey []byte) ([]byte, error) {
	v, err := t.reader.Get(context.TODO(), dataKey)
	if kv.ErrNotExist.Equal(err) {
		err = nil
		v = nil
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	return v, nil
}
