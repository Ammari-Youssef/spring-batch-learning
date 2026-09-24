package com.youssef.batch;

import org.springframework.batch.item.ItemProcessor;

public class PersonProcessor implements ItemProcessor<Person, Person> {
    @Override
    public Person process(Person person) {
        person.setName(person.getName().trim().toUpperCase());
        person.setCity(person.getCity().trim());
        return person;
    }
}
