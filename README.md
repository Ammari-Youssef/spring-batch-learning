# spring-batch-learning

A hands-on repo for learning **Spring Batch** step by step, one milestone at a time.
Each milestone builds on the previous one and ends with a clean commit + a working,
verifiable demo.

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

> Note: in M3 the job auto-runs at startup and takes the `failAtRow` parameter from
> the command line (no `--` prefix). Since M4 the job only runs when POSTed
> (`job.enabled: false`).

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
│   ├── BatchConfig.java        # Job / Step / reader / processor / writer beans
│   └── PersonProcessor.java    # transformation + failAtRow failure injection
└── person/
    ├── Person.java             # JPA entity
    └── PersonController.java   # POST /api/jobs?fileName=&failAtRow=  (job launcher)

src/main/resources/
├── application.yml             # H2 + JPA + batch config
└── data/people.csv             # 10,000 rows: name,age,city
```

## What's next

- Async launch (give the `JobLauncher` a `TaskExecutor`) so a POST returns instantly
- Skip & retry for transient errors
- Scheduling (run jobs on a timer)
- Move to Postgres so job history survives app restarts

## Author

[Ammari-Youssef](https://github.com/Ammari-Youssef)