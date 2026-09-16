package replog

import (
	"os/exec"
	"sort"
	"strings"
	"testing"
)

const modulePath = "github.com/oguzhanozfe/paxos-arena"

// allowedDirectImports is the set the design permits in the protocol core.
var allowedDirectImports = map[string]bool{
	"encoding/json":                true,
	"crypto/sha256":                true,
	"errors":                       true,
	"fmt":                          true,
	"sort":                         true,
	"strconv":                      true,
	"strings":                      true,
	"time":                         true,
	"math/rand/v2":                 true,
	modulePath + "/internal/paxos": true,
}

// TestNoForbiddenImports enforces the dependency direction of the design:
// the protocol core imports only a short list of standard packages and
// internal/paxos, and depends on no other package of this module.
func TestNoForbiddenImports(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go binary not on PATH")
	}
	for _, pkg := range []string{modulePath + "/internal/paxos", modulePath + "/internal/replog"} {
		t.Run(pkg, func(t *testing.T) {
			out, err := exec.Command(goBin, "list", "-f", `{{join .Imports "\n"}}`, pkg).Output()
			if err != nil {
				t.Fatalf("go list imports: %v", err)
			}
			var bad []string
			for _, imp := range strings.Split(strings.TrimSpace(string(out)), "\n") {
				if imp == "" {
					continue
				}
				if !allowedDirectImports[imp] {
					bad = append(bad, imp)
				}
			}
			sort.Strings(bad)
			if len(bad) > 0 {
				t.Errorf("%s imports packages outside the allowed set: %v", pkg, bad)
			}

			out, err = exec.Command(goBin, "list", "-deps", "-f", "{{.ImportPath}}", pkg).Output()
			if err != nil {
				t.Fatalf("go list -deps: %v", err)
			}
			for _, dep := range strings.Split(strings.TrimSpace(string(out)), "\n") {
				if !strings.HasPrefix(dep, modulePath+"/") {
					continue
				}
				if dep != modulePath+"/internal/paxos" && dep != modulePath+"/internal/replog" {
					t.Errorf("%s depends on %s, which is above it in the dependency direction", pkg, dep)
				}
			}
		})
	}
}
