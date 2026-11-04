package advisor

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestGreedyFollowsTheCostsTheBudgetAndTheDeadline(t *testing.T) {
	costs := map[string]float64{"": 100, "0": 80, "1": 50, "2": 95, "0,1": 40, "1,2": 48, "0,2": 76, "0,1,2": 39}
	const base, mb = int64(1 << 30), int64(1 << 20)
	cases := []struct {
		name     string
		sizes    []int64
		budgetMB int
		deadline time.Duration
		chosen   []int
		timedOut bool
	}{
		{"largest improvement first, stop under 10%", []int64{mb, mb, mb}, 0, time.Minute, []int{1, 0}, false},
		{"over the size budget is skipped", []int64{mb, 3 * mb, mb}, 2, time.Minute, []int{0}, false},
		{"past the deadline", []int64{mb, mb, mb}, 0, -time.Second, nil, true},
	}
	for _, c := range cases {
		s := &session{queries: []*workloadQuery{{Query: Query{Text: "q", Weight: 1}}}, costs: map[string][]float64{}, deadline: time.Now().Add(c.deadline)}
		for key, cost := range costs {
			s.costs[key] = []float64{cost}
		}
		for _, size := range c.sizes {
			s.candidates = append(s.candidates, &candidate{size: size})
		}
		chosen, timedOut, err := s.greedy(context.Background(), base, c.budgetMB)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if !reflect.DeepEqual(chosen, c.chosen) || timedOut != c.timedOut {
			t.Errorf("%s: chosen %v, timed out %t; want %v, %t", c.name, chosen, timedOut, c.chosen, c.timedOut)
		}
	}
}
