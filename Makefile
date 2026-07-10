.PHONY: graph watch query explain test lint format

graph:
	graphify update .

watch:
	graphify watch .

query:
	graphify query "$(q)"

explain:
	graphify explain "$(x)"

test:
	uv run pytest test/ -v

lint:
	uv run ruff check .

format:
	uv run ruff format .
