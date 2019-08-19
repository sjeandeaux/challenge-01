VERSION=0.0.0
BUILD_DATE=$(shell date +%Y-%m-%dT%H:%M:%S%z)

BUILD_ARGS=--build-arg VERSION=$(VERSION) --build-arg BUILD_DATE="$(BUILD_DATE)"

# https://gist.github.com/sjeandeaux/e804578f9fd68d7ba2a5d695bf14f0bc
help: ## prints help.
	@grep -hE '^[a-zA-Z_-]+.*?:.*?## .*$$' ${MAKEFILE_LIST} | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: tools
tools: ## install tools to develop
	go get github.com/axw/gocov/gocov
	go get github.com/AlekSi/gocov-xml


.PHONY: dependencies
dependencies: ##Download the dependencies
	go mod download

.PHONY: build
build: 	##Build the binary
	mkdir -p ./target
	CGO_ENABLED=0 go build -installsuffix 'static' -o ./target/ingestator .

.PHONY: fmt
fmt: ## go fmt on packages
	go fmt .

.PHONY: vet
vet: ## go vet on packages
	go vet 

.PHONY: lint
lint: ## go vet on packages
	golint -set_exit_status=true 

.PHONY: test-coverage
test-coverage: clean fmt vet ## coverage test
	gocov test --short -cpu=2 -p=2 -v $(LDFLAGS) | gocov-xml > ./target/coverage-test-it-test.xml

.PHONY: it-test
it-test: ## run integration test
	go test -cpu=2 -p=1


docker-compose-build: ## builds the application image with docker-compose.
	docker-compose build $(BUILD_ARGS)

docker-compose-up: ## spawns the containers.
	VERSION=$(VERSION) BUILD_DATE=$(BUILD_DATE) docker-compose up --abort-on-container-exit --exit-code-from ingestator --build

docker-compose-it-test: ## run integration test in docker-compose
	VERSION=$(VERSION) BUILD_DATE=$(BUILD_DATE) docker-compose -f docker-compose.yml -f docker-compose-integration.yml up --abort-on-container-exit --exit-code-from ingestator --build

clean: ## clean the target folder
	@rm -fr target 
	@mkdir target

