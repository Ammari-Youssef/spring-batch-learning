# Spring Batch — Chunked Processing, Restart, Skip/Retry & Scheduling

A reference implementation of reliable, fault-tolerant batch processing with **Spring Batch 5** and **Spring Boot 3.5.16**. Built incrementally across seven milestones, it demonstrates production-relevant patterns with concrete, verifiable, and runnable demos.

## What it demonstrates

- **Chunk-oriented processing** – CSV ingestion with `FlatFileItemReader`, domain transformation in an `ItemProcessor`, and persistence via `JpaItemWriter` with proper transaction boundaries
- **Reliability & restartability** – Correct `JobInstance` identity, `@StepScope` reader state, and resume-from-last-committed-chunk semantics after a failed execution
- **Resilience & fault tolerance** – `faultTolerant()` with `.skip()`/`.skipLimit()` to drop bad records and `.retry()`/`.retryLimit()` to recover from transient failures without data loss
- **Asynchronous execution** – Offloads job launches to background threads via a `@BatchTaskExecutor`-qualified `TaskExecutor` so REST calls return `202 Accepted` immediately
- **Scheduling** – Cron-based execution with `@EnableScheduling`/`@Scheduled`, plus a `JobExplorer`-backed status endpoint for inspection
- **Operational observability** – Leverages Spring Batch metadata tables to prove restart, skip, retry and execution state in practice

## Tech stack

- Java 17 + Maven wrapper
- Spring Boot 3.5.16
- Spring Batch (chunk-oriented jobs)
- Spring Data JPA (`JpaItemWriter`)
- H2 (embedded, in-memory) — zero-setup DB, includes the Spring Batch metadata schema
- Lombok (`@RequiredArgsConstructor`, `@Getter`/`@Setter`)
- Springdoc OpenAPI + DevTools

## Milestones

| # | Commit summary | What it teaches | How to run |
|---|---|---|---|
| M1 | people CSV import (Jdbc writer) | `Job`/`Step`/`Chunk`, `FlatFileItemReader`, `JdbcBatchItemWriter`, metadata tables | `./mvnw spring-boot:run` |
| M2 | JPA writer | JPA entity + `JpaItemWriter`, auto DDL | `./mvnw spring-boot:run` |
| M3 | job parameters | `JobParameters` from CLI, `@StepScope` + late binding (`@Value("#{jobParameters['failAtRow']}")`), failure injection, chunk rollback | `./mvnw spring-boot:run "-Dspring-boot.run.arguments=failAtRow=5000"` |
| M4 | on-demand launch + restart | `JobLauncher` controller, `JobInstance` identity, restart semantics, `@StepScope` reader for resume | see [Restart demo](#restart-demo) |
| M5 | async launch | `TaskExecutor` bean with `@BatchTaskExecutor` qualifier, POST returns instantly (202), job runs on its own thread | POST `localhost:8080/api/jobs?failAtRow=5000` |
| M6 | skip & retry | `faultTolerant()`, `.skip()/.skipLimit()`, `.retry()/.retryLimit()`, skip/retry counters in the metadata | see [Skip & retry demo](#skip--retry-demo) |
| M7 | scheduling + status | `@EnableScheduling` + `@Scheduled(cron)`, `JobExplorer` status endpoint | set `batch.import.cron`, then `./mvnw spring-boot:run` |

> Note: in M3 the job auto-runs at startup and takes the `failAtRow` parameter from
> the command line (no `--` prefix). Since M4 the job only runs when POSTed
> (`job.enabled: false`).

### Async launch (M5)

By default the `JobLauncher` runs synchronously on the caller's thread (the HTTP
request). Giving it a `@BatchTaskExecutor`-qualified `TaskExecutor` bean makes each
launch run on its own `batch-1, batch-2, ...` thread:

- The POST answers immediately with `202 Accepted`, the execution id, and status
  `STARTING`/`STARTED` — the job keeps running afterwards.
- A re-POST while it is still running → `409 Job already running`.
- Once it reaches `COMPLETED`, a re-POST → `409 Job already complete` (same instance).
- A launch that fails at `failAtRow=5000` still fails in the background… and the same URL re-POSTed
  picks up the resume — all as in M4, just off the request thread.

Gotchas that make this non-trivial: Boot only picks up a TaskExecutor **qualified**
with `@BatchTaskExecutor` for its `TaskExecutorJobLauncher` (else it silently stays
synchronous), and the bean must live in its own dependency-free `@Configuration`
(`AsyncConfig.java`) to avoid a circular dependency with `BatchConfig`.

## Restart demo (M4)

The whole point of M4: same params = same `JobInstance`; a FAILED instance can be
restarted and resumes from the last committed chunk; a COMPLETED instance refuses a
rerun.

1. Start the app: `./mvnw spring-boot:run`
2. Launch with a failure: `POST localhost:8080/api/jobs?failAtRow=5000` (see `demo.http` in IntelliJ)
3. Re-POST the **same URL** → the job resumes from row 4901 and completes
4. POST a third time → rejected: `JobInstanceAlreadyCompleteException`

Verify in the H2 console (`http://localhost:8080/h2-console`, JDBC URL
`jdbc:h2:mem:testdb`, user `sa`, empty password):

```sql
-- identity: one instance, several executions (FAILED -> FAILED -> COMPLETED)
SELECT JOB_INSTANCE_ID, JOB_NAME FROM BATCH_JOB_INSTANCE;
SELECT JOB_EXECUTION_ID, JOB_INSTANCE_ID, START_TIME, STATUS FROM BATCH_JOB_EXECUTION ORDER BY JOB_EXECUTION_ID;
-- resume proof: READ 5000 -> 5100 (resumed, not rerun!) -> 200
SELECT STEP_EXECUTION_ID, READ_COUNT, WRITE_COUNT, ROLLBACK_COUNT, STATUS FROM BATCH_STEP_EXECUTION ORDER BY STEP_EXECUTION_ID;
-- where Spring stores the resume cursor (the reader's position per execution)
SELECT STEP_EXECUTION_ID, SHORT_CONTEXT FROM BATCH_STEP_EXECUTION_CONTEXT ORDER BY STEP_EXECUTION_ID;
-- all committed chunks eventually = all 10,000 rows in the file (4900+4900+200)
SELECT COUNT(*) AS PERSISTED_ROWS FROM people;
```

With `failAtRow=5000` and `chunk(100)`: row 5000 is the last row of chunk #50
(4901–5000); the whole chunk rolls back, so only 4900 rows persist on a run that
fails exactly there.

## Skip & retry demo (M6)

Before M6 any exception killed the job. A real job meets garbage rows and flaky
databases, so the step is made **fault tolerant** (`BatchConfig.importStep`):

```java
.faultTolerant()
.skipLimit(10)                        // give up after 10 skipped items
.skip(IllegalArgumentException.class) // bad records -> drop that item, keep going
.retry(CannotAcquireLockException.class) // transient DB error -> re-try the chunk
.retryLimit(3)                       // give up after 3 attempts
```

Two different ideas, two different parameters:

- **skip** = *this item is bad, never going to work.* Drop it, keep the rest.
- **retry** = *this failure may be temporary.* Re-run the whole chunk and hope it
  succeeds next time.

Skip and retry both work by **rolling the chunk back and re-processing it** — for a
skip the offending item is excluded on the second pass, for a retry it is processed
normally. So neither produces duplicate rows, and neither is free (extra reads +
rollbacks).

### Skip demo — bad records in the file

`skipEvery=N` makes the processor throw on every Nth row (a malformed record).

```bash
POST localhost:8080/api/jobs?failAtRow=0&skipEvery=1000   # 10 bad rows -> COMPLETED, 9990 rows
POST localhost:8080/api/jobs?failAtRow=0&skipEvery=100    # 1000 bad rows -> FAILED at skip limit
```

The limit is the whole point:

| `skipEvery` | bad rows | outcome | rows in DB |
|---|---|---|---|
| 1000 | 10 | COMPLETED (limit not exceeded) | 9990 |
| 10 | 100 | FAILED inside chunk 2 | 90 |
| 2 | 5000 | FAILED inside chunk 1 | 0 |

Note the third row: the job dies **in the first chunk** because the 11th bad row
appears at row 22, so nothing ever commits. `skipEvery=2` is a good reminder that
skip is *tolerance*, not filtering — if half your data is bad you want the job to
fail, not to quietly persist half of it. To genuinely drop rows, return `null` from
the processor (counted as `FILTER_COUNT`).

### Retry demo — flaky database

`FlakyWriter` wraps the real `JpaItemWriter` and throws `CannotAcquireLockException`
("row locked by another transaction") on the first `flakyWrites` write calls, then
behaves normally. It simulates a transient DB failure that a retry can fix.

```bash
POST localhost:8080/api/jobs?failAtRow=0&skipEvery=0&flakyWrites=2  # COMPLETED, 10000 rows
POST localhost:8080/api/jobs?failAtRow=0&skipEvery=0&flakyWrites=5  # FAILED, retryLimit exceeded
```

Verify in H2 (`http://localhost:8080/h2-console`):

```sql
-- skip counters (PROCESS_SKIP = bad rows dropped by the processor)
SELECT STEP_EXECUTION_ID, READ_COUNT, WRITE_COUNT, PROCESS_SKIP_COUNT, ROLLBACK_COUNT, STATUS
FROM BATCH_STEP_EXECUTION ORDER BY STEP_EXECUTION_ID;
-- retried chunks roll back and are re-read, so READ_COUNT can exceed the row count
SELECT COUNT(*) AS PERSISTED_ROWS FROM people;
```

Run with `--logging.level.org.springframework.batch.core=DEBUG` to watch it happen:
`SimpleRetryExceptionHandler: Handled non-fatal exception` for retries,
`Rollback for ... Skipped at row N` for skips.

**Rule of thumb:** retry *transient* problems (locks, timeouts, network blips); never
retry a permanent business error — it will just burn attempts. And never `skip` a
**writer** exception (the chunk may already be part-committed); skip is for
reader/processor faults.

## Scheduling (M7)

Until M7 the job only ever ran because you POSTed it. `ScheduleConfig` launches it
on a timer instead, so a real import happens without anyone watching:

```java
@Scheduled(cron = "${batch.import.cron}")
public void runImport() {
    jobLauncher.run(peopleJob, new JobParametersBuilder()
            .addString("fileName", "people.csv")
            .addLong("run.id", System.currentTimeMillis())
            .toJobParameters());
}
```

The cron lives in `application.yml` (`batch.import.cron`). Note it is **6 fields**,
not 5 — Spring adds seconds: `sec min hour dom mon dow`. Useful test values:

| cron | meaning |
|---|---|
| `"*/5 * * * * *"` | every 5 seconds (fastest way to see it fire) |
| `"0 * * * * *"` | every minute |
| `"0 0 2 * * *"` | 02:00:00 daily (the committed value) |

### The parameter lesson (this is why `run.id` exists)

`run.id` is a **timestamp**, so each night is a *new* `JobInstance` and the job really
runs. Change it to a fixed value:

```java
.addLong("run.id", 1L)   // one instance, ever
```

…and the first fire imports, while every fire after that throws
`JobInstanceAlreadyCompleteException` — the scheduler being refused because that
instance is already `COMPLETED`. Same rule as M4, just applied to a timer: **the
parameters decide the identity, and a fixed set means "run once".**

Because those exceptions are checked, the launcher is wrapped in a try/catch. Letting
them escape a `@Scheduled` method is a real bug — Spring's scheduler will log the
stack trace every time it fires.

### Checking a run without the H2 console

```bash
curl localhost:8080/api/jobs/1
```

`GET /api/jobs/{executionId}` looks the execution up through `JobExplorer`, so you can
read the status of a scheduled *or* POSTed run without opening SQL.

## Getting started

```bash
./mvnw test                 # runs the app + job (context test)
./mvnw spring-boot:run      # start the app
```

## Project layout

```
src/main/java/com/youssef/batch/
├── BatchApplication.java       # Spring Boot entry point
├── config/                     # batch wiring
│   ├── AsyncConfig.java        # @BatchTaskExecutor TaskExecutor (async launches)
│   ├── BatchConfig.java        # Job / Step / reader / processor / writer beans
│   ├── FlakyWriter.java        # wraps JpaItemWriter, fails transiently (retry demo)
│   ├── PersonProcessor.java    # transformation + failAtRow / skipEvery injection
│   └── ScheduleConfig.java     # @Scheduled cron launch of peopleJob
└── person/
    ├── Person.java             # JPA entity
    └── PersonController.java   # POST /api/jobs (launch) + GET /api/jobs/{id} (status)

src/main/resources/
├── application.yml             # H2 + JPA + batch config
└── data/people.csv             # 10,000 rows: name,age,city
```

## What's next

- Move to Postgres so job history survives app restarts (config-only change —
  H2 is in-memory, so every restart currently wipes the job tables)

## Author

[Ammari-Youssef](https://github.com/Ammari-Youssef)