# gofd

All in one and cross-platform cli for file management.

## Compile

```bash
go build .
```

## Usage

### Find files

```bash
# Output all files in PATH
gofd find <PATH>

# Find only files in PATH
gofd find -t file <PATH>
gofd find -t f <PATH>

# Find only directories in PATH
gofd find -t dir <PATH>
gofd find -t d <PATH>

# Find empty directories in PATH
gofd find -t empty <PATH>

# Find empty directories in PATH and delete them
gofd find -t empty -x delete <PATH>
gofd find -t empty -x rm <PATH>

# Copy files
gofd find -x copy-to:<DIR> <PATH>

# Move files
gofd find -x move-to:<DIR> <PATH>
```

### File deduplication

```bash
# Delete duplicate files in dir2 if the file can be found in dir1
gofd dedup file <DIR1> <DIR2>
```

### Merge two directories

```bash
# merge dir2 to dir1
gofd merge <DIR1> <DIR2>
```

### File statistics

```bash
# Statistics file sizes in PATH
gofd stat <PATH>
```

### File hash

```bash
# calculate file hash with XXHash
gofd hash xxh <PATH>
```
