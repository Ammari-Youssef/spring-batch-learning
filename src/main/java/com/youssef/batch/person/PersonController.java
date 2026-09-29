package com.youssef.batch.person;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class PersonController {

    private final JobLauncher jobLauncher;
    private final Job peopleJob;


    @PostMapping
    public ResponseEntity<String> runJob(
            @RequestParam(defaultValue = "people.csv") String fileName,
            @RequestParam(defaultValue = "0") int failAtRow) {

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("fileName", fileName)
                .addLong("failAtRow", (long) failAtRow)
                .toJobParameters();

        try {
            JobExecution execution = jobLauncher.run(peopleJob, jobParameters);

            return ResponseEntity.accepted()
                    .body("Job started | execution id " + execution.getId()
                            + " | status " + execution.getStatus());

        } catch (JobInstanceAlreadyCompleteException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body("Job already complete");
        } catch (JobExecutionAlreadyRunningException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body("Job already running");
        } catch (JobParametersInvalidException e) {
            return ResponseEntity.badRequest().body("Invalid job parameters: " + e.getMessage());
        } catch (JobRestartException e) {
            return ResponseEntity.internalServerError().body("Restart failed: " + e.getMessage());
        }
    }


}
