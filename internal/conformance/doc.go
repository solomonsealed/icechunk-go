// Package conformance holds the tests that check this implementation
// against upstream Icechunk: its on-disk compatibility fixtures
// (testdata/upstream), repositories written by icechunk-python
// (testdata/generated), what icechunk-python reads from them
// (testdata/oracle), repositories Go writes for icechunk-python to verify
// (testdata/check_go_*.py), and writer scenarios run by both writers
// (testdata/writer). It has no code of its own.
package conformance

// Fixture paths. Tests run in this directory; testdata is at the module root.
const (
	repoRoot = "../.."
	testdata = repoRoot + "/testdata"
)
