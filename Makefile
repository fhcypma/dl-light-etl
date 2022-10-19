install:
	pipenv install -d
	pipenv shell || echo "Continuing"

install-ci:
	python -m pip install pipenv
	python -m pipenv lock -r > requirements.txt
	python -m pip install -r requirements.txt

test:
	pytest -vvv --doctest-modules --junitxml=junit/test-results.xml --cov=. --cov-report=xml

code:
	black dl_light_etl --check
	flake8 dl_light_etl
	# mypy dl_light_etl

# Just for local build
build: clean
	python -m build -C--global-option=egg_info -C--global-option=--tag-build=dev12345.0 --wheel

clean:
	@rm -rf .pytest_cache/ .mypy_cache/ junit/ build/ dist/
	@find . -not -path './.venv*' -path '*/__pycache__*' -delete
	@find . -not -path './.venv*' -path '*/*.egg-info*' -delete