# Contributing

## Local checks

Use Python 3.11 or newer, then run:

```bash
python3 -m unittest discover -s tests -v
docker compose -f docker/docker-compose.yml config -q
```

## Changes to Raft

Raft changes must preserve the core safety properties: monotonic terms, one vote per term, log matching, majority commit, and applying only committed entries. Add a deterministic test for every behavior change; transport-dependent tests should not be the only coverage.

## Pull requests

Keep pull requests focused, explain the scenario being changed, include test output, and update the README when changing the protocol, command line, Docker deployment, or guarantees.
