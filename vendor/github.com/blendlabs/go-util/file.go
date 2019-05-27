package util

import (
	"bufio"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/blendlabs/go-exception"
)

const (
	// Kilobyte represents the bytes in a kilobyte.
	Kilobyte int64 = 1 << 10
	// Megabyte represents the bytes in a megabyte.
	Megabyte int64 = Kilobyte << 10
	// Gigabyte represents the bytes in a gigabyte.
	Gigabyte int64 = Megabyte << 10
)

// File contains helper functions for files.
var File = &fileUtil{}

type fileUtil struct{}

// CreateOrOpen creates or opens a file.
func (fu fileUtil) CreateOrOpen(filePath string) (*os.File, error) {
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if os.IsNotExist(err) {
		return os.Create(filePath)
	}
	return f, err
}

// CreateAndClose creates and closes a file.
func (fu fileUtil) CreateAndClose(filePath string) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	return nil
}

// RemoveMany removes an array of files.
func (fu fileUtil) RemoveMany(filePaths ...string) error {
	var err error
	for _, path := range filePaths {
		err = os.Remove(path)
		if err != nil {
			return err
		}
	}
	return err
}

func (fu fileUtil) List(path string, expr *regexp.Regexp) ([]string, error) {
	var files []string
	dirFiles, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var fileBase string
	for _, dirFile := range dirFiles {
		if dirFile.IsDir() {
			continue
		}
		fileBase = filepath.Base(dirFile.Name())
		if expr == nil {
			files = append(files, fileBase)
			continue
		}
		if expr.MatchString(fileBase) {
			files = append(files, fileBase)
		}
	}
	return files, err
}

func (fu fileUtil) ParseSize(fileSizeValue string, defaultFileSize int64) int64 {
	if len(fileSizeValue) == 0 {
		return defaultFileSize
	}

	if len(fileSizeValue) < 2 {
		val, err := strconv.Atoi(fileSizeValue)
		if err != nil {
			return defaultFileSize
		}
		return int64(val)
	}

	units := strings.ToLower(fileSizeValue[len(fileSizeValue)-2:])
	value, err := strconv.ParseInt(fileSizeValue[:len(fileSizeValue)-2], 10, 64)
	if err != nil {
		return defaultFileSize
	}
	switch units {
	case "gb":
		return value * Gigabyte
	case "mb":
		return value * Megabyte
	case "kb":
		return value * Kilobyte
	}
	fullValue, err := strconv.ParseInt(fileSizeValue, 10, 64)
	if err != nil {
		return defaultFileSize
	}
	return fullValue
}

// ReadFile reads a file as a string.
func (fu fileUtil) ReadString(path string) (string, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// ReadChunkHandler is a receiver for a chunk of a file.
type ReadChunkHandler func(line []byte) error

//ReadLineHandler is a receiver for a line of a file.
type ReadLineHandler func(line string) error

// ReadFileByLines reads a file and calls the handler for each line.
func (fu fileUtil) ReadByLines(filePath string, handler ReadLineHandler) error {
	if f, err := os.Open(filePath); err == nil {
		defer f.Close()

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			err = handler(line)
			if err != nil {
				return exception.Wrap(err)
			}
		}
	} else {
		return exception.Wrap(err)
	}
	return nil
}

// ReadFileByChunks reads a file in `chunkSize` pieces, dispatched to the handler.
func (fu fileUtil) ReadByChunks(filePath string, chunkSize int, handler ReadChunkHandler) error {
	if f, err := os.Open(filePath); err == nil {
		defer f.Close()

		chunk := make([]byte, chunkSize)
		for {
			readBytes, err := f.Read(chunk)
			if err == io.EOF {
				break
			}
			readData := chunk[:readBytes]
			err = handler(readData)
			if err != nil {
				return exception.Wrap(err)
			}
		}
	} else {
		return exception.Wrap(err)
	}
	return nil
}

// ReadAllBytes reads all bytes in a file.
func (fu fileUtil) ReadAllBytes(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return ioutil.ReadAll(file)
}
