package tools

import (
	"strings"
	"testing"
	"time"
)

func TestSwitchVerdictGoesByThePrimaryFirst(t *testing.T) {
	before := primaryState{primary: "zone1/a", term: 7}
	cases := []struct {
		after primaryState
		want  []string
	}{
		{primaryState{primary: "zone1/b", term: 7}, []string{"the primary changed from zone1/a to zone1/b", "term 7 has not risen yet"}},
		{primaryState{primary: "zone1/b", term: 8}, []string{"the primary changed from zone1/a to zone1/b"}},
		{primaryState{primary: "zone1/a", term: 8}, []string{"the same node was elected again; the primary did not change"}},
		{primaryState{primary: "zone1/a", term: 7}, []string{"the primary did not change within 30s"}},
		{primaryState{primary: "", term: 8}, []string{"shows no primary after 30s; whether the switch finished is unknown"}},
	}
	for _, c := range cases {
		text := strings.Join(switchVerdict(before, c.after, 30*time.Second), "\n")
		for _, want := range c.want {
			if !strings.Contains(text, want) {
				t.Errorf("after %+v: %q has no %q", c.after, text, want)
			}
		}
		if c.after.primary != before.primary && strings.Contains(text, "did not change") {
			t.Errorf("after %+v: a changed primary is reported as unchanged: %q", c.after, text)
		}
	}
}
