# spring-batch-learning

A hands-on repo for learning **Spring Batch** with Spring Boot 4.1.1.
Currently a clean scaffold with **no business logic** — the ideal place to start
experimenting step by step.

## Tech stack

- Java 17 + Maven wrapper
- Spring Boot 3.5.16
- Spring Batch (with Batch Test support)
- H2 (embedded, in-memory) — zero-setup database for learning
- Postgres driver included, ready for when you switch to a real DB
- Springdoc OpenAPI for future REST endpoints

## Getting started

```bash
./mvnw spring-boot:run
```

The H2 embedded database is auto-configured and Spring Batch automatically
creates its metadata tables, so you can start writing jobs immediately.

Run the tests:

```bash
./mvnw test
```

## What I plan to learn

1. Core concepts: `Job`, `Step`, `Chunk`, `JobLauncher`, Job/Step listeners
2. Item readers / processors / writers (file, CSV, JPA, JDBC)
3. Job parameters, flow & conditional execution
4. Restart, skip & retry behavior
5. Scheduling jobs and testing with `spring-boot-starter-batch-test`
6. Moving from H2 to Postgres

## Project layout

```
src/main/java/com/youssef/batch/   # Spring Boot application
src/main/resources/                # application.properties (H2 default)
```

## Author

[Ammari-Youssef](https://github.com/Ammari-Youssef)