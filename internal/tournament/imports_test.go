package tournament

import (
	"os/exec"
	"sort"
	"strings"
	"testing"
)

const modulePath = "github.com/oguzhanozfe/paxos-arena"

// allowedDirectImports is the set the design permits in the domain core:
// a short list of standard packages plus the packages below it
// (internal/jsonx is the strict JSON decoder every layer shares).
// encoding/hex renders the input digest the design specifies as hex.
// crypto/hmac and encoding/binary derive and expand the deal seeds of
// package game (docs/UNITY-INTEGRATION.md section 5).
var allowedDirectImports = map[string]bool{
	"encoding/json":                 true,
	"encoding/hex":                  true,
	"encoding/binary":               true,
	"crypto/hmac":                   true,
	"crypto/sha256":                 true,
	"errors":                        true,
	"fmt":                           true,
	"sort":                          true,
	"strconv":                       true,
	"strings":                       true,
	"time":                          true,
	"math/rand/v2":                  true,
	modulePath + "/internal/jsonx":  true,
	modulePath + "/internal/paxos":  true,
	modulePath + "/internal/ledger": true,
	modulePath + "/internal/game":   true,
}

// allowedModuleDeps is the closure of module packages the domain core may
// depend on, directly or transitively.
var allowedModuleDeps = map[string]bool{
	modulePath + "/internal/jsonx":      true,
	modulePath + "/internal/paxos":      true,
	modulePath + "/internal/ledger":     true,
	modulePath + "/internal/game":       true,
	modulePath + "/internal/tournament": true,
}

// TestNoForbiddenImports enforces the dependency direction of the design
// for game, ledger and tournament: jsonx, paxos <- ledger <- tournament and
// game <- tournament, with no import of replog, transport, replica, api,
// session, intent or sim.
func TestNoForbiddenImports(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go binary not on PATH")
	}
	for _, pkg := range []string{modulePath + "/internal/game", modulePath + "/internal/ledger", modulePath + "/internal/tournament"} {
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
				// game sits beside ledger, below tournament: it depends on
				// no other package of the module.
				if !allowedModuleDeps[dep] || (pkg == modulePath+"/internal/game" && dep != pkg) {
					t.Errorf("%s depends on %s, which is above it in the dependency direction", pkg, dep)
				}
			}
		})
	}
}
