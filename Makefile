run:
	uv run src/main.py

format_file:
	uv run ruff format

generate_files:
	uv run src/file_generator.py $(path)