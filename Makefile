.PHONY: help tests build
default: help

help:
	@echo '>make help 				/ this screen'
	@echo '>make setup				/ installs virtualenv, requirements'
	@echo '>make tests				/ builds, runs unit tests'
	@echo '>make run   				/ runs the client main (not much done here)'
	@echo '>make build				/ builds the package'
	@echo '>make upload				/ uploads package to pypi'

setup:
	python3 -m venv .venv3 && . .venv3/bin/activate && pip install -r requirements.txt

tests:
	python -m unittest discover

run:
	. .venv3/bin/activate && CONDUIT_SERVER=${CONDUIT_SERVER} CONDUIT_TOKEN=${CONDUIT_TOKEN} python src/conduit_pkg/client.py

build:
	rm -rf dist/*
	. .venv3/bin/activate && python3 -m build

upload:
	. .venv3/bin/activate &&  python3 -m twine upload dist/*