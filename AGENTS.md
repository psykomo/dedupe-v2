# Project Instructions: dedupe-gemini

This project uses **uv** for dependency management and **typer** for the CLI.

## Tooling & Commands
- **Dependency Manager**: `uv`
- **CLI Framework**: `typer`
- **Environment**: Always use `uv run <command>` to execute scripts.
- **Entry Point**: The CLI is defined as `dedupe` in `pyproject.toml`, pointing to `dedupe_gemini:app`.

## Development Workflow
- To add a package: `uv add <package>`
- To run the CLI: `uv run dedupe`
- To sync dependencies: `uv sync`

## Code Structure
- All source code resides in the `dedupe_gemini/` directory.
- The main entry point for the Typer app is `dedupe_gemini/__init__.py`.
