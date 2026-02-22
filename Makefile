.PHONY: install install-dev validate deploy-dry deploy status
install:
	pip install -e .
install-dev:
	pip install -e ".[all]"
validate:
	cassachange validate
deploy-dry:
	cassachange deploy --dry-run --dry-run-output migration-plan.json
deploy:
	cassachange deploy
status:
	cassachange status
