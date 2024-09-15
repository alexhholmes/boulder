package compression

// Some sort of wrapper around a few compression algos. This will be used for
// compressing the SSTable files during the compaction process. Should
// level 0 files be compressed?

// This could be an "option" passed to the storage by the file writer owner.
// Manifest or file footer would contain the compression type used for the file.
