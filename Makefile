ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

build:
	go mod tidy
	go build ./cmd/pgroute66

debug:
	go build -gcflags "all=-N -l" ./cmd/pgroute66
	${GOBIN}/dlv --headless --listen=:2345 --api-version=2 --accept-multiclient exec ./pgroute66 -- -c ./config/pgroute66_local.yaml

fmt:
	golangci-lint run --fix
	gofmt -w .
	gofumpt -l -w .
	goimports -w .
	gci write .

.PHONY: test
test: ## Run tests.
	go test $$(go list ./... | grep -v e2e) -coverprofile cover.out

.PHONY: install-go-test-coverage
install-go-test-coverage:
	go install github.com/vladopajic/go-test-coverage/v2@latest

.PHONY: check-coverage
check-coverage: install-go-test-coverage
	go test $$(go list ./... | grep -v e2e) -coverprofile=./cover.out -covermode=atomic -coverpkg=./...
	${GOBIN}/go-test-coverage --config=./.testcoverage.yaml

e2e-test: db2e2e-test pge2e-test

.PHONY: db2-e2e-test
db2e2e-test: ## Run tests.
	go test -v ./tests/db2e2e

.PHONY: pg-e2e-test
pge2e-test: ## Run tests.
	go test -v ./tests/pge2e
