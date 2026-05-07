package model

import (
	"fmt"
	"strings"
)

func CanonicalEventKey(kind string, repo EventRepoRef, target EventTargetRef, subscope string) string {
	key := fmt.Sprintf("%s:%s/%s/%s:%s:%s", strings.TrimSpace(kind), strings.TrimSpace(repo.Host), strings.TrimSpace(repo.Owner), strings.TrimSpace(repo.Name), strings.TrimSpace(target.Key), strings.TrimSpace(target.Fingerprint))
	if strings.TrimSpace(subscope) != "" {
		key += ":" + strings.TrimSpace(subscope)
	}
	return key
}
