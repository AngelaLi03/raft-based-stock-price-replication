# CI/CD Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add GitHub Actions CI that runs the test suite and a linter on every push/PR, builds and (on `main`) publishes the Docker image to GHCR, and document the one-time manual branch-protection setup that makes checks required before merge.

**Architecture:** One workflow file, `.github/workflows/ci.yml`, with three jobs — `test` and `lint` run in parallel and independently; `docker-build-push` depends on both (`needs: [test, lint]`) and only actually pushes an image on `push` events to `main`, not on PRs or other branches.

**Tech Stack:** GitHub Actions, `actions/setup-python@v5`, `docker/build-push-action`, ruff (new dependency), GHCR (`ghcr.io`).

## Global Constraints

- ruff's rule set is exactly `E4,E7,E9,F` (pyflakes + basic pycodestyle) — no expanded/opinionated rule categories (pyupgrade, bandit, pylint-equivalent, etc.) for this feature.
- Generated protobuf files (`raft/proto/*_pb2.py`, `raft/proto/*_pb2_grpc.py`, `client/proto/*_pb2.py`, `client/proto/*_pb2_grpc.py`) are excluded from lint entirely — never hand-fix generated code.
- Docker image pushes only on `push` to `main`; PRs and other branches build-only (no push), and GHCR is the only registry — no Docker Hub, no new secrets to configure.
- Python 3.11 in every job that needs it — matches the pinned `grpcio==1.59.0`, which has no Python 3.13 wheel.
- Full test suite (165 tests) must stay green throughout every task in this plan.
- This plan executes in an isolated git worktree/branch (never on `main` directly), following this repo's established convention. Commit normally at the end of each task inside the worktree.
- **Pushing this branch to `origin` (required to verify GitHub Actions actually runs — Actions doesn't execute on purely local commits) needs explicit go-ahead before it happens** — standing rule for this repo this session. Everything through Task 3 is committed locally only; Task 4 is the push and must not be dispatched without the user's explicit confirmation first.

---

### Task 1: Add ruff, fix all existing lint violations

**Files:**
- Create: `ruff.toml`
- Modify: `requirements.txt`
- Modify (lint fixes only, no behavior change): `kv/state_machine.py`, `raft/node.py`, `raft/prometheus_metrics.py`, `raft/storage.py`, `raft/structured_logging.py`, `raft/types.py`, `scripts/benchmark.py`, `scripts/chaos_test.py`, `server/metrics_server.py`, `tests/test_batch_replication.py`, `tests/test_chaos_recovery.py`, `tests/test_crash_recovery.py`, `tests/test_election.py`, `tests/test_idempotent_apply.py`, `tests/test_kv_apply.py`, `tests/test_performance_metrics.py`, `tests/test_persistence.py`, `tests/test_raft_node.py`, `tests/test_replication.py`, `tests/test_snapshotting.py`, `tests/test_structured_logging.py`

**Interfaces:**
- Produces: a repo where `ruff check .` exits 0 (matches what Task 2's `lint` job will run) and `PYTHONPATH=. pytest tests/ -v` still shows 165 passed.

- [ ] **Step 1: Create `ruff.toml`**

```toml
target-version = "py311"

exclude = [
    "raft/proto/*_pb2.py",
    "raft/proto/*_pb2_grpc.py",
    "client/proto/*_pb2.py",
    "client/proto/*_pb2_grpc.py",
]

[lint]
select = ["E4", "E7", "E9", "F"]
```

- [ ] **Step 2: Add `ruff` to `requirements.txt`**

Append a new line to the end of `requirements.txt`:

```
ruff==0.16.2
```

- [ ] **Step 3: Install ruff and confirm the baseline violation count**

```bash
pip install -r requirements.txt
ruff check .
```

Expected: 47 errors (2 more, in `raft/proto/raft_pb2.py` and `client/proto/client_pb2.py`, are already excluded by `ruff.toml` and won't appear). If the count differs from 47, stop and report — it means the codebase has drifted since this plan was written; don't blindly proceed against a different baseline than what's documented here.

- [ ] **Step 4: Auto-fix everything ruff can fix safely**

```bash
ruff check . --fix
```

This mechanically fixes all `F401` (unused import) violations — 41 of the 47. Verify: `ruff check .` should now show only `F841` (unused variable) violations remaining.

- [ ] **Step 5: Manually fix the remaining 6 unused-variable (`F841`) violations**

These aren't auto-fixable because removing a variable assignment requires confirming nothing later in the function references it — read each site before deleting:

- `scripts/benchmark.py:507` — `results` assigned but never used. Read the surrounding function; remove the assignment (either delete the line, or if the right-hand side has a needed side effect, keep the call but drop the `results =` assignment).
- `tests/test_crash_recovery.py:300` — `entry2` assigned but never used.
- `tests/test_crash_recovery.py:301` — `entry3` assigned but never used.
- `tests/test_crash_recovery.py:365` — `entry2_new` assigned but never used.
- `tests/test_structured_logging.py:35` — `context_dict` assigned but never used.
- `tests/test_structured_logging.py:333` — `handler` assigned but never used.

For each: open the file, look at the assignment and the surrounding ~15 lines, confirm the variable is genuinely dead (not referenced later in the same scope — `ruff check .` already did this analysis, but verify before deleting since the fix is manual, not tool-applied), and remove the now-unused assignment. If the right-hand side of the assignment is a call with a needed side effect (e.g. `handler = logging.StreamHandler(log_capture)` might need `logging.StreamHandler(log_capture)` to still run even if the return value isn't used, or might be genuinely fully removable if `log_capture` is what actually matters for the test) — read the test's intent before deciding whether to keep a bare call or delete the whole line.

- [ ] **Step 6: Verify lint is fully clean**

```bash
ruff check .
```

Expected: `All checks passed!`

- [ ] **Step 7: Verify the fixes didn't break anything**

```bash
PYTHONPATH=. pytest tests/ -v
```

Expected: 165 passed. If anything fails, an import removed in Step 4/5 was not actually unused — investigate the specific failure, don't just revert broadly.

- [ ] **Step 8: Commit**

```bash
git add ruff.toml requirements.txt kv/state_machine.py raft/node.py raft/prometheus_metrics.py raft/storage.py raft/structured_logging.py raft/types.py scripts/benchmark.py scripts/chaos_test.py server/metrics_server.py tests/
git commit -m "Add ruff, fix all 47 existing lint violations

Generated protobuf files excluded from lint scope entirely (raft/proto,
client/proto) - hand-fixing generated code is pointless and any fix
would be lost on the next scripts/gen_protos.sh run. The remaining
violations (41 unused imports, 6 unused local variables) are all
mechanical, no behavior change - full test suite still green."
```

---

### Task 2: GitHub Actions workflow (test, lint, docker-build-push)

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: `ruff.toml` and the clean lint state from Task 1; `requirements.txt` (now including `ruff`); `scripts/gen_protos.sh`; `ops/Dockerfile`.
- Produces: three named checks (`test`, `lint`, `docker-build-push`) that will appear on every push and PR once this branch is pushed to GitHub (verified in Task 4, not this task — Actions doesn't run on purely local commits).

- [ ] **Step 1: Write the workflow file**

Create `.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: ['**']
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Regenerate protobuf stubs
        run: bash scripts/gen_protos.sh
      - name: Run tests
        run: PYTHONPATH=. pytest tests/ -v

  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Install ruff
        run: pip install ruff
      - name: Run ruff
        run: ruff check .

  docker-build-push:
    runs-on: ubuntu-latest
    needs: [test, lint]
    permissions:
      contents: read
      packages: write
    steps:
      - uses: actions/checkout@v4
      - name: Build image
        run: docker build -f ops/Dockerfile -t raft-node:ci .
      - name: Log in to GHCR
        if: github.event_name == 'push' && github.ref == 'refs/heads/main'
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - name: Tag and push image
        if: github.event_name == 'push' && github.ref == 'refs/heads/main'
        run: |
          IMAGE=ghcr.io/angelali03/raft-node
          docker tag raft-node:ci "$IMAGE:latest"
          docker tag raft-node:ci "$IMAGE:${{ github.sha }}"
          docker push "$IMAGE:latest"
          docker push "$IMAGE:${{ github.sha }}"
```

- [ ] **Step 2: Validate the YAML is well-formed**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))" && echo "valid YAML"
```

Expected: `valid YAML`. This only catches syntax errors, not logic errors — the real test is Task 4's actual push.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "Add GitHub Actions CI: test, lint, and docker-build-push jobs

docker-build-push depends on both test and lint passing (needs: [test,
lint]) so a broken build or lint failure never results in a published
image. Only pushes to GHCR on push events to main - PRs and other
branches build-only, verifying the Dockerfile still builds without
publishing anything."
```

---

### Task 3: Document CI in README

**Files:**
- Modify: `README.md`

**Interfaces:**
- None (documentation only).

- [ ] **Step 1: Add a "Continuous Integration" section**

Insert a new section in `README.md` right after the "## Running the Test Suite" section ends and before "## Running the Cluster" begins (currently: line 154's test-count sentence, then a blank line, then `## Running the Cluster` at line 156):

```markdown

## Continuous Integration

GitHub Actions (`.github/workflows/ci.yml`) runs on every push and on every PR targeting `main`:

- **`test`** — installs `requirements.txt`, regenerates protobuf stubs with the pinned toolchain (`scripts/gen_protos.sh`), runs the full `pytest` suite.
- **`lint`** — runs `ruff check .` (`ruff.toml` pins the rule set to `E4`/`E7`/`E9`/`F` and excludes generated protobuf files).
- **`docker-build-push`** — depends on both `test` and `lint` passing. Builds `ops/Dockerfile` on every push/PR (catches a broken Dockerfile before merge); on pushes to `main` only, also publishes `ghcr.io/angelali03/raft-node:latest` and `:<git-sha>`.

### Enabling required status checks (one-time, manual)

GitHub branch protection isn't configurable from a workflow file — this is a one-time repo setting:

1. GitHub repo → Settings → Branches → Add branch protection rule
2. Branch name pattern: `main`
3. Enable "Require status checks to pass before merging"
4. Select `test` and `lint` as required checks
5. Save
```

- [ ] **Step 2: Remove "CI/CD" from Future Roadmap**

In the `## Future Roadmap (not started)` section, change:

```
- **Visualization & Deployment**: FastAPI + Chart.js dashboard showing live price charts and cluster/leader-election status, CI/CD
```

to:

```
- **Visualization & Deployment**: FastAPI + Chart.js dashboard showing live price charts and cluster/leader-election status
```

- [ ] **Step 3: Add `.github/` and `ruff.toml` to Project Structure**

In the `## Project Structure` tree, add a new top-level entry before `├── proto/` (or wherever fits alphabetically/logically at the top of the tree):

```
├── .github/
│   └── workflows/
│       └── ci.yml              # test, lint, docker-build-push (GHCR, main only)
```

And add a line for `ruff.toml` near the bottom of the tree, alongside other top-level config files (next to wherever `README.md` is listed):

```
├── ruff.toml                   # Lint rule set (E4/E7/E9/F), excludes generated protobuf files
```

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "README.md: document CI/CD (workflow jobs, branch protection setup)"
```

---

### Task 4: Push and verify live on GitHub

**This task requires explicit go-ahead before it starts** — it's the first push to `origin` this plan makes, and GitHub Actions consuming real Actions minutes is a visible, shared-state action, not a local one.

**Files:** None new — this is verification of Tasks 1-3's already-committed work.

**Interfaces:** None.

- [ ] **Step 1: Push the branch**

```bash
git push -u origin <branch-name>
```

- [ ] **Step 2: Confirm all three jobs actually ran and passed**

Check the GitHub Actions tab for this push (or `gh run list` / `gh run watch` if the `gh` CLI is available in the execution environment — it was not available when this plan was written, so this may need to be checked manually in the GitHub UI instead). Expected: `test`, `lint`, and `docker-build-push` (build-only, since this isn't `main`) all show green.

- [ ] **Step 3: Deliberately verify the checks can actually fail**

Not just "the YAML parses" — prove a real regression gets caught:

```bash
# Reintroduce one unused import temporarily
echo "import sys  # deliberately unused, for CI verification" >> raft/types.py
git add raft/types.py
git commit -m "test: verify lint job catches a real violation (temporary)"
git push
```

Confirm the `lint` job goes **red** on GitHub Actions for this push. Then revert:

```bash
git revert HEAD --no-edit
git push
```

Confirm `lint` goes green again on the revert commit's check run.

- [ ] **Step 4: Open a PR against `main` and confirm checks appear**

Create a PR from this branch to `main` (via `gh pr create` if available, or the GitHub UI). Confirm `test` and `lint` show up as checks on the PR itself, not just on the branch's own commits.

- [ ] **Step 5: Report back what's still manual**

This task does not merge the PR or enable branch protection — both are explicit follow-up actions for the repo owner:
- Enabling branch protection per the README's documented steps (Task 3, Step 1) is a manual GitHub UI action, not something this plan automates.
- Merging this PR (which would be the first real push to `main` that triggers `docker-build-push`'s actual GHCR publish step) is a separate decision requiring its own explicit go-ahead, same as the push in Step 1.
