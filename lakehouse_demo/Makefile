PYTHON ?= python

.PHONY: setup run ingest silver gold mart test lint

setup:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e .[dev]

run:
	$(PYTHON) -m lakehouse all

ingest:
	$(PYTHON) -m lakehouse ingest

silver:
	$(PYTHON) -m lakehouse silver

gold:
	$(PYTHON) -m lakehouse gold

mart:
	$(PYTHON) -m lakehouse mart

test:
	$(PYTHON) -m pytest -q

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m black --check .
