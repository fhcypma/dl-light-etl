install:
	pip install -r requirements.txt

test: install
	pytest -vvv --doctest-modules --junitxml=junit/test-results.xml --cov=. --cov-report=xml

code: install
	black dl_light_etl --check
	flake8 dl_light_etl
	# mypy dl_light_etl

build: clean
	python -m build

clean:
	@rm -rf .pytest_cache/ .mypy_cache/ junit/ build/ dist/
	@find . -not -path './.venv*' -path '*/__pycache__*' -delete
	@find . -not -path './.venv*' -path '*/*.egg-info*' -delete