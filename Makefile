# Standard library only, so the gates are gofmt, go vet, the race detector
# and the tests. `make check` mirrors .github/workflows/go.yml.

GO ?= go
BIN ?= bin

.PHONY: all build test race lint tidy check sim-long fuzz e2e clean

all: check

## build: compile every package and put the two commands in $(BIN)/.
build:
	$(GO) build ./...
	mkdir -p $(BIN)
	$(GO) build -o $(BIN)/arena ./cmd/arena
	$(GO) build -o $(BIN)/chaos ./cmd/chaos

## test: run the tests once, without the race detector (about 17 s).
test:
	$(GO) test -count=1 ./...

## race: run the tests once under the race detector, in shuffled order (about 2.5 min).
## internal/sim alone takes close to 2.5 min per -count under -race, so keep an
## explicit -timeout above 10m when raising -count.
race:
	$(GO) test -race -count=1 -shuffle=on -timeout 15m ./...

## lint: gofmt, go vet and a tidy go.mod. No third-party linters are used.
lint:
	test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }
	$(GO) vet ./...
	$(GO) mod tidy -diff

## tidy: rewrite go.mod if needed (lint only checks).
tidy:
	$(GO) mod tidy

## check: everything CI runs.
check: lint build race

## sim-long: the simulation sweep with 1000 seeds per scenario and 2000 random seeds.
sim-long:
	$(GO) test -count=1 -timeout 60m -run 'TestScenarios|TestRandom' ./internal/sim -seeds=1000 -random-seeds=2000

## fuzz: run each fuzz target for 30 s.
fuzz:
	$(GO) test -run=NONE -fuzz=FuzzDecode -fuzztime=30s ./internal/replog
	$(GO) test -run=NONE -fuzz=FuzzDecode -fuzztime=30s ./internal/tournament
	$(GO) test -run=NONE -fuzz=FuzzSeed -fuzztime=30s ./internal/sim

## e2e: the client SDK's harness against three arena processes, once with the
## leader killed mid-round (needs curl and a .NET SDK; skips without dotnet).
## Not part of check or CI.
e2e:
	scripts/e2e.sh

clean:
	rm -rf $(BIN) arena chaos
	$(GO) clean -testcache
