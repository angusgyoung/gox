package operator

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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

// TestBuildFetchPendingEventsQuery_MultipleTopics tests with multiple topics separately
// because map iteration order is non-deterministic, so we check for the presence of
// each condition rather than an exact string match.
func TestBuildFetchPendingEventsQuery_MultipleTopics(t *testing.T) {
	result := buildFetchPendingEventsQuery(map[string][]int{
		"topic-a": {1},
		"topic-b": {2, 3},
	})

	normalized := normalize(result)
	assert.Contains(t, normalized, normalize("(topic = 'topic-a' AND partition IN (1))"))
	assert.Contains(t, normalized, normalize("(topic = 'topic-b' AND partition IN (2,3))"))
	assert.Contains(t, normalized, "or")
}

func normalize(s string) string {
	return strings.ToLower(strings.Join(strings.Fields(s), ""))
}
