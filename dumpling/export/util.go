// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"github.com/pingcap/tidb/br/pkg/version"
)

func sameStringArray(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func string2Map(a, b []string) map[string]string {
	a2b := make(map[string]string, len(a))
	for i, str := range a {
		a2b[str] = b[i]
	}
	return a2b
}

func needRepeatableRead(serverType version.ServerType, consistency string) bool {
	return consistency != ConsistencyTypeSnapshot || serverType != version.ServerTypeTiDB
}

func infiniteChan[T any]() (chan<- T, <-chan T) {
	in, out := make(chan T), make(chan T)

	go func() {
		var (
			q  []T
			e  T
			ok bool
		)
		handleRead := func() bool {
			if !ok {
				for _, e = range q {
					out <- e
				}
				close(out)
				return true
			}
			q = append(q, e)
			return false
		}
		for {
			if len(q) > 0 {
				select {
				case e, ok = <-in:
					if handleRead() {
						return
					}
				case out <- q[0]:
					q = q[1:]
				}
			} else {
				e, ok = <-in
				if handleRead() {
					return
				}
			}
		}
	}()
	return in, out
}
