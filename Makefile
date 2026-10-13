PROFILE  ?= academy
REGION   ?= us-east-1
RATE     ?= 100
DURATION ?= 300

TFVARS := -var="profile=$(PROFILE)" -var="region=$(REGION)"
VENV   := .venv
PY     := $(VENV)/bin/python

.PHONY: help whoami init plan deploy simulate burst dashboard grafana test local-demo dashboards fmt destroy

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-12s %s\n", $$1, $$2}'

whoami: ## Show which AWS account the credentials point to
	aws sts get-caller-identity --profile $(PROFILE)

init: ## Create/reuse this account's state bucket and initialize Terraform
	PROFILE=$(PROFILE) REGION=$(REGION) scripts/bootstrap-backend.sh

plan: ## Preview the changes
	terraform plan $(TFVARS)

deploy: ## Create or update the whole stack
	terraform apply $(TFVARS)

simulate: $(VENV) ## Run the sensor fleet against Kinesis (RATE readings/s for DURATION s)
	$(PY) simulator/sensor_fleet.py --profile $(PROFILE) --region $(REGION) --rate $(RATE) --duration $(DURATION)

burst: ## Big ingestion burst: 1,000 readings/s for 2 minutes
	$(MAKE) simulate RATE=1000 DURATION=120

dashboard: ## Open the planner-facing web app
	open "$$(terraform output -raw dashboard_url)"

grafana: ## Print the Grafana URL and admin password, and open it
	@echo "Grafana: $$(terraform output -raw grafana_url)   user: admin   password: $$(terraform output -raw grafana_admin_password)"
	open "$$(terraform output -raw grafana_url)"

test: $(VENV) ## Run all tests on mocked AWS (no account needed): Python pipeline + terraform test
	PYTHONDONTWRITEBYTECODE=1 $(PY) -m unittest discover -s tests -v
	terraform init -backend=false -input=false >/dev/null
	terraform test

local-demo: $(VENV) ## Run the whole pipeline locally on mocked AWS, web app on http://localhost:8080
	PYTHONDONTWRITEBYTECODE=1 $(PY) scripts/local_demo.py

dashboards: ## Regenerate grafana/dashboards/*.json from grafana/build_dashboards.py
	python3 grafana/build_dashboards.py

fmt: ## Format Terraform files
	terraform fmt -recursive

destroy: ## Delete everything (do this when you finish: Kinesis and Redshift bill by the hour)
	terraform destroy $(TFVARS)

$(VENV): requirements-dev.txt
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q -r requirements-dev.txt
	touch $(VENV)
