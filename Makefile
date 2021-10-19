release: .pipenv ensure-java-exists lint coverage sdist

sdist:
	pipenv run python setup.py sdist

quick-upload:
	pipenv run python setup.py sdist 	
	pipenv run twine check dist/*
	pipenv run twine upload dist/* --verbose

upload: release quick-upload

clean:
	pipenv --rm || true
	rm -rf .pipenv .pytest_cache ddb_local.egg-info htmlcov dist/
	find -name '__pycache__' | xargs rm -fr

.pipenv:
	pipenv install -d
	touch "$@"

coverage: .pipenv
	pipenv run coverage run -m pytest -v
	coverage html

test: .pipenv
	pipenv run pytest -xv

lint: .pipenv
	pipenv run black ddb_local/ test/

ensure-java-exists:
	which java > /dev/null || echo "Can't find java in PATH"

.PHONY: test clean ensure-java-exists coverage lint sdist upload quick-upload