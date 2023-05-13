package internal

import (
	"strings"
	"testing"
)

type buildFetchPendingEvensQueryTest struct {
	input map[string][]int
	want  string
}

var testBuildFetchPendingEventsQueryProvider = []buildFetchPendingEvensQueryTest{
	{
		input: map[string][]int{
			"test-topic-a": {1, 2, 3},
		},
		want: `
		SELECT * FROM outbox 
		WHERE status = $1
		AND ((topic = 'test-topic-a' AND partition IN (1,2,3)))
		ORDER BY created_timestamp ASC
		LIMIT $2`,
	},
	{
		input: map[string][]int{
			"test-topic-a": {1, 2, 3},
			"test-topic-b": {5, 6},
		},
		want: `
		SELECT * FROM outbox 
		WHERE status = $1
		AND ((topic = 'test-topic-a' AND partition IN (1,2,3)) OR 
			(topic = 'test-topic-b' AND partition IN (5,6)))
		ORDER BY created_timestamp ASC
		LIMIT $2`,
	},
	{
		input: map[string][]int{
			"test-topic-a": {1, 2, 3},
			"test-topic-b": {5, 6},
			"test-topic-c": {7},
		},
		want: `
		SELECT * FROM outbox 
		WHERE status = $1
		AND ((topic = 'test-topic-a' AND partition IN (1,2,3)) OR 
			(topic = 'test-topic-b' AND partition IN (5,6)) OR
			(topic = 'test-topic-c' AND partition IN (7)))
		ORDER BY created_timestamp ASC
		LIMIT $2`,
	},
}

func TestBuildFetchPendingEventsQuery(t *testing.T) {
	for _, test := range testBuildFetchPendingEventsQueryProvider {
		normalisedWant := normalise(test.want)
		normalisedGot := normalise(buildFetchPendingEventsQuery(test.input))
		if normalisedWant != normalisedGot {
			t.Fatalf("\nwanted: '%s'\ngot: '%s'", normalisedWant, normalisedGot)
		}
	}
}

func normalise(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	s = strings.ReplaceAll(s, "\n", " ")

	return s
}
