clean:
	@rm -rf build dist .eggs *.egg-info
	@rm -rf .benchmarks .coverage coverage.xml htmlcov report.xml .tox
	@find . -type d -name '.mypy_cache' -exec rm -rf {} +
	@find . -type d -name '__pycache__' -exec rm -rf {} +
	@find . -type d -name '*pytest_cache*' -exec rm -rf {} +
	@find . -type f -name "*.py[co]" -exec rm -rf {} +

style:
	black .
	isort .
	flake8 --exclude=scrape_data/* notebooks/*
	@echo "The style pass! ✨ 🍰 ✨"

# check: test lint style
# check: test lint style mypy