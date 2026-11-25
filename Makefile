.PHONY: check test lock-binaries build image-check image-check-worker image-check-control-plane

GO := env GOTOOLCHAIN=go1.26.4 go

check:
	@test "$$($(GO) env GOVERSION)" = go1.26.4 || { echo 'Go 1.26.4 is required' >&2; exit 1; }
	@toolchain_gofmt=$$($(GO) env GOROOT)/bin/gofmt; files=$$("$$toolchain_gofmt" -l cmd internal third_party/actions-scaleset/*.go third_party/actions-scaleset/internal) && test -z "$$files" || { printf '%s\n' "$$files"; exit 1; }
	$(GO) vet ./...
	cd third_party/actions-scaleset && $(GO) vet ./...

# -count=1 is load-bearing, not just a -race habit: internal/admission and
# internal/backend/kubernetes read fixtures under hosts/ms-a2/, which sits
# outside this module. Go's test cache keys on package source only, so an
# edit there is invisible to it -- without -count=1 a staged break in those
# fixtures can read green off a cached pass (homelab#1006). Do not drop it.
test:
	$(GO) test -race -count=1 ./...
	cd third_party/actions-scaleset && $(GO) test -race -count=1 -run '^(TestResume|TestCreateMessageSession|TestGetMessage|TestDeleteMessage|TestAcquireJobs|TestPinnedUpstreamSnapshot)' .

# Output lives outside the checkout by default; no binary is committed or
# published by this target. Both targets use the same Go source and module pin.
OUTPUT_DIR ?= /private/tmp/slipway-build

# The four linux/amd64 binaries the image locks pin, and nothing else. The
# `Runner pool image lock` CI job builds only these: the lock hashes no other
# output, and the rest of `build` (both `./...` compiles and the darwin
# binaries) is about three quarters of its compile time. `build` depends on
# this target rather than repeating the lines, so round.sh and the lock job
# cannot drift onto different flags.
lock-binaries:
	mkdir -p "$(OUTPUT_DIR)"
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -buildvcs=false -o "$(OUTPUT_DIR)/runner-pool-linux-amd64" ./cmd/runner-pool
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -buildvcs=false -o "$(OUTPUT_DIR)/runner-pool-worker-linux-amd64" ./cmd/runner-pool-worker
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -buildvcs=false -o "$(OUTPUT_DIR)/runner-pool-host-witness-linux-amd64" ./cmd/runner-pool-host-witness
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -buildvcs=false -o "$(OUTPUT_DIR)/runner-pool-host-tool-linux-amd64" ./cmd/runner-pool-host-tool

build: lock-binaries
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build ./...
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(GO) build ./...
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(GO) build -trimpath -buildvcs=false -o "$(OUTPUT_DIR)/runner-pool-darwin-arm64" ./cmd/runner-pool
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(GO) build -trimpath -buildvcs=false -o "$(OUTPUT_DIR)/runner-pool-worker-darwin-arm64" ./cmd/runner-pool-worker

image-check:
	image/linux/check-lock.sh --source-only
	python3 image/linux/test_bundle.py -v
	python3 image/check-control-plane.py
	python3 image/test_control_plane.py -v
	python3 image/check-review-lock.py
	python3 image/test_review_lock.py -v

image-check-worker:
	@test -n "$(WORKER_BINARY)" || { echo 'WORKER_BINARY is required' >&2; exit 2; }
	image/linux/check-lock.sh --worker "$(WORKER_BINARY)"

image-check-control-plane:
	@test -n "$(CONTROLLER_BINARY)" || { echo 'CONTROLLER_BINARY is required' >&2; exit 2; }
	@test -n "$(WITNESS_BINARY)" || { echo 'WITNESS_BINARY is required' >&2; exit 2; }
	@test -n "$(HOST_TOOL_BINARY)" || { echo 'HOST_TOOL_BINARY is required' >&2; exit 2; }
	python3 image/check-control-plane.py --controller "$(CONTROLLER_BINARY)" --witness "$(WITNESS_BINARY)" --host-tool "$(HOST_TOOL_BINARY)"
