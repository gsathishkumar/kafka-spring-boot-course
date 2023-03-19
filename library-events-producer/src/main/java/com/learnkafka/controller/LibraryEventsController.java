package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@RestController
@Slf4j
public class LibraryEventsController {

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> createLibraryEvents(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {

        libraryEvent.setLibraryEventType(LibraryEventType.NEW);

        // Approach - 1 (Async without headers)
        // libraryEventProducer.sendLibraryEventsAsync_Approach1(libraryEvent);

        // Approach - 2 (Async with headers)
         libraryEventProducer.sendLibraryEventsAsync_Approach2(libraryEvent);

        // Approach - 3 (Sync without headers)
        // SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventsSync(libraryEvent);
        // log.info("SendResult is {}", sendResult);

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> updateLibraryEvents(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {
        if(libraryEvent.getLibraryEventId() == null){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the libraryEventId");
        }
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEventProducer.sendLibraryEventsAsync_Approach2(libraryEvent);
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }
}
