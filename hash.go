package main

import (
	"crypto/sha256"
	"encoding/binary"
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

func sha256ByteSlice(buf []byte) []uint64 {
	h := sha256.New()
	h.Write(buf)
	b := h.Sum(nil)
	s := [4]uint64{}
	for i := 0; i < len(s); i++ {
		start := i * 8
		end := start + 8
		s[i] = binary.BigEndian.Uint64(b[start:end])
	}
	return s[:]
}
