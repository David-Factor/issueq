// Package deps anchors Phase 0 dependency choices until implementation packages use them directly.
package deps

import (
	gogithub "github.com/google/go-github/v66/github"
	"github.com/oklog/ulid/v2"
	"gopkg.in/yaml.v3"

	_ "modernc.org/sqlite"
)

var (
	_ gogithub.Issue
	_ ulid.ULID
	_ yaml.Node
)
