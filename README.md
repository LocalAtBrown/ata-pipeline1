# ata-pipeline1 (EDA Branch)

<!-- [![Release](https://img.shields.io/github/v/release/LocalAtBrown/ata-pipeline1)](https://img.shields.io/github/v/release/LocalAtBrown/ata-pipeline1) -->
<!-- [![Build status](https://img.shields.io/github/workflow/status/LocalAtBrown/ata-pipeline1/merge-to-main)](https://img.shields.io/github/workflow/status/LocalAtBrown/ata-pipeline1/merge-to-main) -->

[![Python version](https://img.shields.io/badge/python_version-3.9.-blue)](https://github.com/psf/black)
[![Code style with black](https://img.shields.io/badge/code_style-black-000000.svg)](https://github.com/psf/black)
[![More style with flake8](https://img.shields.io/badge/code_style-flake8-blue)](https://flake8.pycqa.org)
[![Imports with isort](https://img.shields.io/badge/%20imports-isort-blue)](https://pycqa.github.io/isort/)
[![Type checking with mypy](https://img.shields.io/badge/type_checker-mypy-blue)](https://mypy.readthedocs.io)
[![License](https://img.shields.io/github/license/LocalAtBrown/ata-pipeline1)](https://img.shields.io/github/license/LocalAtBrown/ata-pipeline1)

Exploratory data analysis for Automating the Ask.

## Usage

The Python source code (inside `ata_pipeline1`) is primarily intended to be run in Jupyter Notebooks which show the process and results of the different steps of the EDA.

## Development

This project uses [Poetry](https://python-poetry.org/) to manage dependencies. It also helps with pinning dependency and python
versions. We also use [pre-commit](https://pre-commit.com/) with hooks for [isort](https://pycqa.github.io/isort/),
[black](https://github.com/psf/black), and [flake8](https://flake8.pycqa.org/en/latest/) for consistent code style and
readability for both the `ata_pipeline` source code and Jupyter notebooks. Note that this means code that doesn't meet the rules will fail to commit until it is fixed.

We use [mypy](https://mypy.readthedocs.io/en/stable/index.html) for static type checking. This can be run [manually](#run-static-type-checking),
and the CI runs it on PRs to the `main` branch. We also use [pytest](https://docs.pytest.org/en/7.2.x/) to run our tests.
This can be run [manually](#run-tests) and the CI runs it on PRs to the `main` branch.

### Setup

1. [Install Poetry](https://python-poetry.org/docs/#installation).
2. Run `poetry install --no-root`
3. Run `source $(poetry env list --full-path)/bin/activate && pre-commit install && deactivate` to set up `pre-commit`

You're all set up! Your local environment should include all dependencies, including dev dependencies like `black`.
This is done with Poetry via the `poetry.lock` file.

### IMPORTANT: Privacy Warnings

The Local News Lab at the Brown Institute for Media Innovation dedicates itself to building open-sourced ML products to support local newsrooms and their businesses. Toward this mission, we've made this repository public.

If you contribute to this branch, you'll notice we've done the following things to keep data and information about our partners from leaking as part of the EDA process:

- Hiding the `notebook/data` directory, where we keep our fetched (more below) as well as intermediate (checkpoint) datasets, by specifying it in `.gitignore`.
- Adding a custom pre-commit hook to clear all output in Jupyter notebooks (see `.pre-commit-config.yaml`).

Please abide by these decisions as well as exercise good judgment before you commit.

### Fetching Data

TODO

### Run Code Format and Linting

To manually run isort, black, and flake8 all in one go, simply run `pre-commit run --all-files`. Explore the `pre-commit` docs (linked above)
to see more options.

### Run Static Type Checking

To manually run mypy, simply run `mypy` from the root directory of the project. It will use the default configuration
specified in `pyproject.toml`.

### Update Dependencies

To update dependencies in your local environment, make changes to the `pyproject.toml` file then run `poetry update` from the root directory of the project.

### Run Tests

To manually run rests, simply run `pytest tests` from the root directory of the project. Explore the `pytest` docs (linked above)
to see more options.

### Run Notebooks

Notebooks are located in the `notebooks` directory. To launch a Jupyter server, run `jupyter notebook`.
