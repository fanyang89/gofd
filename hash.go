package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"

	"github.com/cespare/xxhash"
)

func getFileHash(path string) (uint64, error) {
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

func getBufferHash(buf []byte) string {
	h := sha256.New()
	h.Write(buf)
	x := h.Sum(nil)
	return hex.EncodeToString(x)
}
