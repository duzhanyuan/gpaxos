package util

import (
  "os"
)

func DeleteDir(path string) error {
  return os.RemoveAll(path)
}

func IsDirectoryExist(path string) bool {
  file, err := os.Stat(path)
  if err != nil {
    return false
  }

  return file.Mode().IsDir()
}