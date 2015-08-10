package logopher

import (
	"log"
	"testing"
)

func TestLogopher(t *testing.T) {
	w, err := DialUDP("ADDRESS", true)
	if err != nil {
		t.Error(err)
	}
	num, err := w.Log("Hello Smithers, you're quite good at turning me on")
	log.Printf("Wrote: %d", num)
	if err != nil {
		t.Error(err)
	}
}
