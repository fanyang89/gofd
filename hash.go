package main

import (
	"crypto/sha256"
	"io"
	"os"

	"github.com/cespare/xxhash"
)

func xxHashFile(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	h := xxhash.New()

	_, err = io.Copy(h, f)
	if err != nil {
		return 0, err
	}

	return h.Sum64(), nil
}

func xxHashString(s string) uint64 {
	h := xxhash.New()
	_, err := io.WriteString(h, s)
	if err != nil {
		panic(err)
	}
	return h.Sum64()
}

func sha256ByteSlice(buf []byte) []byte {
	h := sha256.New()
	h.Write(buf)
	return h.Sum(nil)
}
