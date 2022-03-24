package canal

import "regexp"

var (
	hookRe *regexp.Regexp
)

func init() {
	hookRe = regexp.MustCompile(`^(\w+)\((.*)\)$`)
}
