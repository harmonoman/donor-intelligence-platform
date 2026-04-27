# python-devcontainer-template

A minimal, reproducible Python dev container template using [uv](https://github.com/astral-sh/uv) for dependency management. Clone once, use forever.

---

## What's Included

| File | Purpose |
|---|---|
| `.devcontainer/devcontainer.json` | VS Code container config, extensions, settings |
| `.devcontainer/Dockerfile` | Python 3.12 slim + uv |
| `.devcontainer/docker-compose.yml` | Workspace mount and .venv volume |
| `.devcontainer/postCreate.sh` | Runs `uv sync` on first container open |
| `pyproject.toml` | Project metadata and dependency config |
| `.python-version` | Pins Python 3.12 for uv |
| `.gitignore` | Python gitignore + `.venv/` |

---

## Using This Template

### 1. Create a new repo from this template

On GitHub, click **"Use this template"** → **"Create a new repository"**.

> Do not fork — the template feature gives you a clean commit history with no connection back to this repo.

### 2. Open in dev container

In VS Code, open the repo and select **"Reopen in Container"** when prompted (or via the Command Palette: `Dev Containers: Reopen in Container`).

The container will build and `postCreate.sh` will run `uv sync` automatically.

### 3. Commit the lockfile

`uv sync` generates `uv.lock` on first run. Commit it so future installs are reproducible:

```bash
git add uv.lock
git commit -m "chore: add uv lockfile"
```

---

## Adding Dependencies

```bash
# Add a runtime dependency
uv add polars

# Add a dev-only dependency
uv add --dev pytest ruff

# Sync after manually editing pyproject.toml
uv sync
```

---

## Requirements

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [VS Code](https://code.visualstudio.com/) with the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)