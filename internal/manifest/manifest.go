package manifest

type Manifest struct {
}

// Manifest recieves info about file changes from the WAL, LSM, and DB (db is
// specific for other data types like indexes, cache bloom filters, etc.)
