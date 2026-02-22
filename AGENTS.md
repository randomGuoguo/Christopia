# Repository Guidelines

## Project Structure & Module Organization

- `modeling_pl/` contains the Python modules; each `.py` file is a standalone utility (e.g., `iv.py`, `ks.py`, `lgb_modeling.py`).
- `modeling_pl/his/` stores historical or archived scripts (e.g., `his/iv_20251105.py`).
- There is no `tests/` directory or package layout (`__init__.py`), so treat the code as a collection of importable modules used by notebooks or other projects.

## Build, Test, and Development Commands

- No build scripts or task runners were found (no `pyproject.toml`, `requirements.txt`, or `Makefile`).
- Run modules directly when needed, or import them from your own scripts by adding the repo to `PYTHONPATH`.
  - Example: `python -c "import sys; sys.path.append('modeling_pl'); import iv"`
- Dependencies are implied by imports in the source (e.g., `polars`, `numpy`, `lightgbm`, `scikit-learn`). Manage the environment manually.

## Coding Style & Naming Conventions

- Use 4-space indentation and snake_case for functions and variables (consistent with existing modules).
- Keep function signatures explicit; many functions accept `df`, column-name strings, and optional parameters.
- Prefer small, focused utilities that operate on `polars` DataFrames.
- No formatter or linter configuration is present; if you add one, keep it lightweight and document it here.

## Testing Guidelines

- No testing framework or test suite is present.
- If you add tests, place them under `tests/` and use a clear naming pattern such as `test_<module>.py`.
- Document the command to run tests once added (e.g., `pytest`).

## Commit & Pull Request Guidelines

- This directory is not a Git repository, so commit conventions and PR requirements cannot be inferred.
- If you initialize Git, use short, imperative commit messages (e.g., "Add PSI calculation") and include:
  - A brief summary of changes.
  - Any relevant data assumptions or input formats.
  - Example usage or validation steps.

## Configuration & Data Notes

- Many utilities expect column names to be passed explicitly; document required columns in function docstrings.
- Avoid hard-coded paths; prefer `Path` objects or parameters (see `lgb_modeling.py` for patterns).
