package deduplication

import "testing"

func TestClassify(t *testing.T) {
	if got := Classify("same", "same"); got != Duplicate {
		t.Fatalf("одинаковые hash классифицированы как %v", got)
	}
	if got := Classify("first", "second"); got != Conflict {
		t.Fatalf("разные hash классифицированы как %v", got)
	}
}
