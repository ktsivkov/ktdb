package storage

import (
	"os"
)

type FileFilter func(entry os.DirEntry) (bool, error)

func IsFileFilter(entry os.DirEntry) (bool, error) {
	return !entry.IsDir(), nil
}

func IsDirFilter(entry os.DirEntry) (bool, error) {
	return entry.IsDir(), nil
}
