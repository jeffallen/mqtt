package mqtt

import (
	"strings"
	"testing"
)

func TestWild(t *testing.T) {
	var tests = []struct {
		wild, topic string
		valid, want bool
	}{
		{"finance#", "", false, false},
		{"finance+", "", false, false},
		{"a/#/b", "", false, false},
		{"finance/stock/ibm/#", "finance/stock", true, false},
		{"finance/stock/ibm/#", "", true, false},
		{"finance/stock/ibm/#", "finance/stock/ibm", true, true},
		{"finance/stock/ibm/#", "", true, false},
		{"#", "anything", true, true},
		{"#", "anything/no/matter/how/deep", true, true},
		{"", "", true, true},
		{"+/#", "one", true, true},
		{"+/#", "", true, true},
		{"finance/stock/+/close", "finance/stock", true, false},
		{"finance/stock/+/close", "finance/stock/ibm", true, false},
		{"finance/stock/+/close", "finance/stock/ibm/close", true, true},
		{"finance/stock/+/close", "finance/stock/ibm/open", true, false},
		{"+/+/+", "", true, false},
		{"+/+/+", "a/b", true, false},
		{"+/+/+", "a/b/c", true, true},
		{"+/+/+", "a/b/c/d", true, false},
	}

	for _, x := range tests {
		w := newWild(x.wild, nil)
		if w.valid() != x.valid {
			t.Fatal("Validation error: ", x.wild)
		}
		got := w.matches(strings.Split(x.topic, "/"))
		if x.want != got {
			t.Error("Fail:", x.wild, x.topic, "got", got)
		}
	}
}
