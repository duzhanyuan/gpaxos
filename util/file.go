package util

import (
  "os"
  "path/filepath"
  "fmt"
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

func IterDir(dirPath string, filePathList []string) error {
  iterFun := func(path string, f os.FileInfo, err error) error {
    if (f == nil) {
      return err
    }
    if f.IsDir() {
      return IterDir(path, filePathList)
    }

    filePathList = append(filepath, f.Name())
    return nil
  }

  return filepath.Walk(dirPath, iterFun)
}