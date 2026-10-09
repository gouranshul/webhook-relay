# Contributing

Thanks for taking a look. This is a small project with a few firm rules; they keep it honest.

## Prerequisites

- JDK 25 (Temurin recommended). Maven is provided by the wrapper (`./mvnw`).
- Docker. Every integration test uses Testcontainers (Postgres, Kafka, Redis, Toxiproxy).

## Build and test

```bash
./mvnw verify                                    # everything, including the chaos tests (~15 min)
./mvnw -pl relay-dispatcher -am verify           # one module and what it depends on
./mvnw -pl relay-e2e verify -Dtest='ChaosTest#kafkaBrokerRestartMidLoad'   # one chaos scenario
```

`verify` also enforces at least 80 % line coverage on `relay-common` and `relay-dispatcher`.

## Running locally

```bash
cp .env.example .env    # replace every placeholder (openssl rand -base64 32 for RELAY_SECRET_KEY)
docker compose up -d --build
```

Benchmarks: `load/run-benchmark.sh <name>` (see `docs/benchmarks.md` for the knobs). Never copy numbers into
docs without the run's output in `load/results/`.

## Ground rules

1. **No secrets in git.** Configuration comes from the environment; `.env.example` holds placeholders only.
   Run `docker run --rm -v "$PWD:/repo" zricethezav/gitleaks:v8.30.1 git /repo` before pushing.
2. **Pin verified versions.** Check Maven Central / Docker Hub / the project's releases before adding or bumping
   a dependency or image. If the current release changed an API, note it in `docs/adr/0007-platform-version-notes.md`.
3. **Delivery semantics are at-least-once.** Do not introduce wording or code that implies exactly-once.
4. **Everything per-endpoint that lives in memory must be bounded**, and every outbound call needs a connect
   and a request timeout.
5. **Conventional Commits** (`feat(dispatcher): ...`, `fix: ...`, `docs(adr): ...`), small and focused.
6. Significant design decisions get an ADR in `docs/adr/`.
