install:
	pip install -r requirements.txt

test:
	pytest -vvv --doctest-modules --junitxml=junit/test-results.xml --cov=. --cov-report=xml

code:
	black .
	flake8

build:
	python3 -m build

.PHONY: init test