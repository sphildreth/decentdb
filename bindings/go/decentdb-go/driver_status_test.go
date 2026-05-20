package decentdb

import (
	"errors"
	"testing"
)

func TestStatusError_QueueClosedMapping(t *testing.T) {
	err := statusError(13, "write queue closed")
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("expected ErrQueueClosed, got %v", err)
	}
}
