package nodes

import "strings"

func MockLLM(input string) string {
	input = strings.ToLower(input)

	switch {
	case strings.Contains(input, "bug"),
		strings.Contains(input, "error"),
		strings.Contains(input, "crash"),
		strings.Contains(input, "broken"):
		return "bug"

	case strings.Contains(input, "bill"),
		strings.Contains(input, "invoice"),
		strings.Contains(input, "payment"),
		strings.Contains(input, "charge"):
		return "billing"

	default:
		return "unclear"
	}
}
