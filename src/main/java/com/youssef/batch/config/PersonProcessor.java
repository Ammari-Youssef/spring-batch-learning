package com.youssef.batch.config;

import com.youssef.batch.person.Person;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.ItemProcessor;

@RequiredArgsConstructor
public class PersonProcessor implements ItemProcessor<Person, Person> {

    private final long failAtRow;
    private long count = 0;

    @Override
    public Person process(Person person) {
        count++;

        if(failAtRow == count && failAtRow > 0) {
            throw new RuntimeException("Failing at row " + count);
        }
        person.setName(person.getName().trim().toUpperCase());
        person.setCity(person.getCity().trim());
        return person;
    }
}
