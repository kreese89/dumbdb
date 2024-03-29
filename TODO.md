# Features
- [] Implement SSTables/LSM-Tree engine
- [] Better encoding for db data files (binary/byte encoded instead of text CSV)
- [x] Reconstruct Hash Index for `NaiveWithHashIndexEngine` on database startup
- [] Add Server support (application accepts connections over TCP)
- [] Add support for usage of multiple data files for an engine
    - [x] `Naive`
    - [x] `NaiveWithHashIndex`
    - [x] Scan for files on startup to populate engine's list of files
- [] Add signal handling/crash detection
- [] Implement compaction
- [] Add threading to separate read/write/compaction operations
- [] Implement B-tree engine
- [] Improve command parsing to allow for spaces in vals (and eventually keys)
- [] (Long term) add distribution (maybe in different repo)
- [] (Long term) Add column support (maybe in different repo)
- [] (Long term) Multi-indexes

# Code Quality/Refactoring
- [] Refactor some shared engine code to defaultly-implemented functions on `Engine`
- [] Make code more idiomatic

# Measurement and Testing
- [] Add basic test suite for `Engine` implementations/structs
- [] Add some sort of measurement/profiling to compare different implementations (potentially a benchmarking bin)