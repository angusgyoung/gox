package internal

import (
	"strings"
	"testing"
)

func TestBuildFetchPendingEventsQuery(t *testing.T) {
	topicPartitions := make(map[string][]int, 2)

	topicPartitions["test-topic-a"] = []int{1, 2, 3}
	topicPartitions["test-topic-b"] = []int{5, 6}

	expected := `
		SELECT * FROM outbox 
		WHERE status = $1
		AND ((topic = 'test-topic-a' AND partition IN (1,2,3)) OR 
			(topic = 'test-topic-b' AND partition IN (5,6)))
		ORDER BY timestamp DESC
		LIMIT $2
	`

	query := buildFetchPendingEventsQuery(topicPartitions)

	normalisedExpected := normalize(expected)
	normalisedQuery := normalize(query)

	if normalisedQuery != normalisedExpected {
		t.Fatalf("\nexpected: '%s'\ngot: '%s'", normalisedExpected, normalisedQuery)
	}
}

func normalize(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	s = strings.ReplaceAll(s, "\n", " ")

	return s
}
