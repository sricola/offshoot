// Package store implements offshoot's object-storage layout: a minimal
// conditional-write Backend interface, the local-directory implementation,
// and the typed manifest/ref schema on top.
package store

import "errors"

var (
	ErrNotFound = errors.New("store: not found")
	ErrCAS      = errors.New("store: compare-and-swap conflict")
)

type Backend interface {
	Get(key string) (data []byte, etag string, err error)
	Put(key string, data []byte) error
	PutIf(key string, data []byte, ifMatch string) (string, error)
	List(prefix string) ([]string, error)
	Delete(key string) error
}
