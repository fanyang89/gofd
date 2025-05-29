package main

import "testing"

func TestChunkDeduplicator_Deduplicate(t *testing.T) {
	d := NewChunkDeduplicator()
	err := d.Deduplicate()
	if err != nil {
		t.Fatal(err)
	}
}
