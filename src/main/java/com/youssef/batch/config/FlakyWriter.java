package com.youssef.batch.config;

import com.youssef.batch.person.Person;
import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.dao.CannotAcquireLockException;

/**
 * Simulates a database that fails transiently on the first `flakyWrites`
 * write() calls (as if a row was locked by another transaction), then behaves
 * normally. Paired with .retry(CannotAcquireLockException.class) on the step,
 * the job survives the hiccup instead of dying on the first write.
 */
@RequiredArgsConstructor
public class FlakyWriter implements ItemWriter<Person> {

    private final EntityManagerFactory entityManagerFactory;
    private final long flakyWrites; // 0 = always healthy
    private int attempts = 0;

    @Override
    public void write(Chunk<? extends Person> items) throws Exception {
        if (attempts < flakyWrites) {
            attempts++;
            throw new CannotAcquireLockException(
                    "Simulated DB lock contention on write attempt " + attempts);
        }

        JpaItemWriter<Person> delegate = new JpaItemWriter<>();
        delegate.setEntityManagerFactory(entityManagerFactory);
        delegate.write(items);
    }
}
