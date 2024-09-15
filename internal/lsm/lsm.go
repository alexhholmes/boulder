package lsm

type LSM struct {
}

// The LSM will run the goroutines for background compaction and serve metrics
// to the DB.
// This tracks number of levels and files at each level.
// It will report all file changes to the manifest.
