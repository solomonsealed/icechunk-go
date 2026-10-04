//go:build !unix

package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"
)

// lockFile takes an exclusive lock by creating path with O_EXCL. A lock
// older than staleLock is assumed to belong to a crashed writer and is
// broken; conditional writes hold it only for milliseconds.
func lockFile(path string) (func(), error) {
	const staleLock = 30 * time.Second
	deadline := time.Now().Add(2 * staleLock)
	for {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			f.Close()
			return func() { os.Remove(path) }, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return nil, err
		}
		if st, err := os.Stat(path); err == nil && time.Since(st.ModTime()) > staleLock {
			os.Remove(path)
			continue
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("storage: timed out waiting for lock %s", path)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
