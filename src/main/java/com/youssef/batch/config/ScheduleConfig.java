package com.youssef.batch.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Slf4j
@Configuration
@EnableScheduling
@RequiredArgsConstructor
public class ScheduleConfig {

    private final JobLauncher jobLauncher;
    private final Job peopleJob;

    /**
     * Runs the import on a cron schedule. run.id is a timestamp so every night is
     * a fresh JobInstance and the job actually runs; a fixed value (e.g. 1L) would
     * give one instance that refuses to re-run once COMPLETED.
     */
    @Scheduled(cron = "${batch.import.cron}")
    public void runImport() {
        try {
            JobExecution execution = jobLauncher.run(peopleJob, new JobParametersBuilder()
                    .addString("fileName", "people.csv")
                    .addLong("run.id", System.currentTimeMillis())
                    .toJobParameters());

            log.info("Scheduled import started, execution id {}", execution.getId());
        } catch (JobInstanceAlreadyCompleteException e) {
            log.info("Scheduled import skipped: this JobInstance already completed");
        } catch (JobExecutionAlreadyRunningException e) {
            log.info("Scheduled import skipped: a previous run is still going");
        } catch (JobRestartException e) {
            log.error("Scheduled import could not be restarted", e);
        } catch (JobParametersInvalidException e) {
            log.error("Scheduled import rejected its own parameters", e);
        }
    }
}
