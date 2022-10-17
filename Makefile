init:
	pip install -r requirements-dev.txt

test:
	pytest -vvv

code:
	black .
	flake8

build:
	python3 -m build

.PHONY: init test