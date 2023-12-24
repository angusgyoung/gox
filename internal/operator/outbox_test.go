package operator

import (
	"strings"
	"testing"
)

func TestBuildFetchPendingEventsQuery(t *testing.T) {
	var tests = []struct {
		name  string
		input map[string][]int
		want  string
	}{
		{
			"Single topic single partition",
			map[string][]int{
				"topic-a": {1},
			},
			`
			SELECT * FROM outbox WHERE status = $1
			AND ((topic = 'topic-a' AND partition IN (1)))
			ORDER BY created_timestamp ASC
			LIMIT $2
			`,
		},
		{
			"Single topic multiple partitions",
			map[string][]int{
				"topic-a": {1, 2, 3},
			},
			`
			SELECT * FROM outbox WHERE status = $1
			AND ((topic = 'topic-a' AND partition IN (1,2,3)))
			ORDER BY created_timestamp ASC
			LIMIT $2
			`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			ans := buildFetchPendingEventsQuery(tt.input)

			actualQuery := normalize(ans)
			desiredQuery := normalize(tt.want)

			if actualQuery != desiredQuery {
				t.Errorf("got %s, want %s", ans, tt.want)
			}
		})
	}
}

func normalize(s string) string {
	return strings.ToLower(strings.Join(strings.Fields(s), ""))
}
