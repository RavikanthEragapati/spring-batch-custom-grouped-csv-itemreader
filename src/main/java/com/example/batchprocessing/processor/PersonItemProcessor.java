package com.example.batchprocessing.processor;

import com.example.batchprocessing.model.Person;
import com.example.batchprocessing.model.Report;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.item.ItemProcessor;

import java.util.List;

public class PersonItemProcessor implements ItemProcessor<List<Person>,Report> {

    private static final Logger log = LoggerFactory.getLogger(PersonItemProcessor.class);

    @Override
    public Report process(final List<Person> personList) {

        Report report = new Report(personList.get(0).id(), true);
        log.info("##########################");
        personList.stream().forEach(item -> log.info("Person object details id : {}, balance : {} ", item.id(), item.balance()));
        log.info("##########################");

        return report;
    }

}
