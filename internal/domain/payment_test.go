package domain

import "testing"

func TestCanTransition(t *testing.T) {
	cases := []struct {
		from, to string
		allowed  bool
	}{
		{StatusCreated, StatusSucceeded, true},
		{StatusCreated, StatusFailed, true},
		{StatusCreated, StatusCreated, true},
		{StatusSucceeded, StatusFailed, false},
		{StatusFailed, StatusSucceeded, false},
	}
	for _, test := range cases {
		if got := CanTransition(test.from, test.to); got != test.allowed {
			t.Errorf("CanTransition(%q,%q)=%t, ожидалось %t", test.from, test.to, got, test.allowed)
		}
	}
}
