DOCKER = docker
DOCKER_COMPOSE = docker-compose
CONTAINER = php-amqp-wrapper

ifndef TESTMETHOD
FILTERARGS=
else
FILTERARGS=--filter $(TESTMETHOD)
endif

.PHONY: all test destroy update

help:
	@echo "Targets:"
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/\(.*\):.*##[ \t]*/	\1 ## /' | column -t -s '##'
	@echo

all: stop destroy build run packages ## Build all

destroy: ## Destroy containers
	-$(DOCKER_COMPOSE) stop
	-$(DOCKER_COMPOSE) rm -f
	-$(DOCKER) rmi $(IMAGE)-base

build: ## Build container
	$(DOCKER) build --no-cache -t $(IMAGE)-base .
	$(DOCKER) build --no-cache -t $(IMAGE) -f Dockerfile.development --build-arg http_proxy=${http_proxy} --build-arg https_proxy=${https_proxy} .

run: ## Run containers
	$(DOCKER_COMPOSE) up -d

stop: ## Stop containers
	$(DOCKER_COMPOSE) stop

restart: stop run ## Restart containers

packages: ## Install packages
	$(DOCKER_COMPOSE) exec $(CONTAINER) /bin/bash -l -c "composer install"

pu: packages-update
packages-update: ## Update packages
	$(DOCKER_COMPOSE) exec $(CONTAINER) /bin/bash -l -c "composer update"

ssh: sh

sh: ## Open sh in the container
	$(DOCKER_COMPOSE) exec $(CONTAINER) /bin/bash

logs: ## Show logs
	$(DOCKER_COMPOSE) logs --follow $(CONTAINER) $(STUB_CONTAINER) $(DB_CONTAINER) 2>&1

test: phpunit ## Run all test

phpunit: ## Test with phpunit
	$(DOCKER_COMPOSE) exec $(CONTAINER) bash -l -c "vendor/bin/phpunit --enforce-time-limit -c test/phpunit.xml $(FILTERARGS) $(TESTFILE)"
