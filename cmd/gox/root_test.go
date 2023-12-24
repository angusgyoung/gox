package gox

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/angusgyoung/gox/internal/operator"
	"github.com/sirupsen/logrus"
)

func TestParseLogLevel(t *testing.T) {
	var tests = []struct {
		input string
		want  logrus.Level
	}{
		{"panic", logrus.PanicLevel},
		{"PANIC", logrus.PanicLevel},
		{"fatal", logrus.FatalLevel},
		{"FATAL", logrus.FatalLevel},
		{"error", logrus.ErrorLevel},
		{"ERROR", logrus.ErrorLevel},
		{"warn", logrus.WarnLevel},
		{"WARN", logrus.WarnLevel},
		{"info", logrus.InfoLevel},
		{"INFO", logrus.InfoLevel},
		{"debug", logrus.DebugLevel},
		{"DEBUG", logrus.DebugLevel},
		{"trace", logrus.TraceLevel},
		{"TRACE", logrus.TraceLevel},
		{"BLAH", logrus.WarnLevel},
		{"", logrus.WarnLevel},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s should result in %s", tt.input, tt.want), func(t *testing.T) {

			ans := parseLogLevel(tt.input)

			if ans != tt.want {
				t.Errorf("got %s, want %s", ans, tt.want)
			}
		})
	}
}

func TestParseLogFormat(t *testing.T) {
	var tests = []struct {
		input string
		want  logrus.Formatter
	}{
		{"text", &logrus.TextFormatter{}},
		{"json", &logrus.JSONFormatter{}},
		{"BLAH", &logrus.TextFormatter{}},
		{"", &logrus.TextFormatter{}},
	}

	for _, tt := range tests {
		desiredType := reflect.TypeOf(tt.want)

		t.Run(fmt.Sprintf("%s should result in %s", tt.input, desiredType), func(t *testing.T) {

			ans := parseLogFormat(tt.input)

			actualType := reflect.TypeOf(ans)

			if actualType != desiredType {
				t.Errorf("got %s, want %s", actualType, desiredType)
			}
		})
	}
}

func TestParseCompletionMode(t *testing.T) {
	var tests = []struct {
		input string
		want  operator.CompletionMode
	}{
		{"update", operator.UpdateCompletionMode},
		{"UPDATE", operator.UpdateCompletionMode},
		{"delete", operator.DeleteCompletionMode},
		{"DELETE", operator.DeleteCompletionMode},
		{"blah", operator.UpdateCompletionMode},
		{"", operator.UpdateCompletionMode},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s should result in %q", tt.input, tt.want), func(t *testing.T) {

			ans := parseCompletionMode(tt.input)

			if ans != tt.want {
				t.Errorf("got %q, want %q", ans, tt.want)
			}
		})
	}
}
