package efsbroker

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
)

//go:generate counterfeiter -o efsbrokerfakes/fake_file_system.go . FileSystem

// Interface on file system calls in order to facilitate testing
type FileSystem interface {
	MkdirAll(string, os.FileMode) error
	TempDir() string
	Stat(string) (os.FileInfo, error)
	RemoveAll(string) error
	Remove(string) error
	Symlink(oldname, newname string) error
	ReadFile(filename string) ([]byte, error)
	WriteFile(filename string, data []byte, perm os.FileMode) error

	// filepath package
	Abs(path string) (string, error)
}

type realFileSystem struct{}

func NewRealFileSystem() realFileSystem {
	return realFileSystem{}
}

func (f *realFileSystem) ReadFile(filename string) ([]byte, error) {
	return ioutil.ReadFile(filename)
}

func (f *realFileSystem) WriteFile(filename string, data []byte, perm os.FileMode) error {
	return ioutil.WriteFile(filename, data, perm)
}

func (f *realFileSystem) MkdirAll(path string, perm os.FileMode) error {
	orig := syscall.Umask(000)
	defer syscall.Umask(orig)

	return os.MkdirAll(path, perm)
}

func (f *realFileSystem) TempDir() string {
	return os.TempDir()
}

func (f *realFileSystem) Stat(path string) (fi os.FileInfo, err error) {
	return os.Stat(path)
}

func (f *realFileSystem) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func (f *realFileSystem) Remove(path string) error {
	return os.Remove(path)
}

func (f *realFileSystem) Abs(path string) (string, error) {
	return filepath.Abs(path)
}

func (f *realFileSystem) Symlink(oldname, newname string) error {
	orig := syscall.Umask(000)
	defer syscall.Umask(orig)

	err := os.Symlink(oldname, newname)
	if err != nil {
		return err
	}

	return os.Chmod(newname, os.ModePerm)
}
