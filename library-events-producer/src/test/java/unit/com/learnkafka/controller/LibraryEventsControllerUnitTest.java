package com.learnkafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
class LibraryEventsControllerUnitTest {

    @Autowired
    MockMvc mockMvc;

    @MockBean
    LibraryEventProducer libraryEventProducer;

    ObjectMapper mapper = new ObjectMapper();

    @Test
    void createLibraryEvents() throws Exception {

        Book book = Book.builder()
                .bookId(12345)
                .bookName("Apache Kafka Spring boot")
                .bookAuthor("Dilip")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        when(libraryEventProducer.sendLibraryEventsAsync_Approach2(isA(LibraryEvent.class))).thenReturn(null);

        String jsonContent = mapper.writeValueAsString(libraryEvent);
        mockMvc.perform(MockMvcRequestBuilders
                        .post("/v1/libraryevent")
                        .content(jsonContent)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());
    }

    @Test
    void createLibraryEvent_withNullBookId_And_NullBookName() throws Exception {

        Book book = Book.builder()
                .bookId(null)
                .bookName(null)
                .bookAuthor("Dilip")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        when(libraryEventProducer.sendLibraryEventsAsync_Approach2(isA(LibraryEvent.class))).thenReturn(null);

        String jsonContent = mapper.writeValueAsString(libraryEvent);
        String expectedErrorMsg = "book.bookId - must not be null,book.bookName - must not be blank";
        mockMvc.perform(MockMvcRequestBuilders
                        .post("/v1/libraryevent")
                        .content(jsonContent)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(expectedErrorMsg));
    }

    @Test
    void updateLibraryEvents() throws Exception {

        Book book = Book.builder()
                .bookId(12345)
                .bookName("Apache Kafka Spring boot")
                .bookAuthor("Dilip")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(99999)
                .book(book)
                .build();

        when(libraryEventProducer.sendLibraryEventsAsync_Approach2(isA(LibraryEvent.class))).thenReturn(null);

        String jsonContent = mapper.writeValueAsString(libraryEvent);
        mockMvc.perform(MockMvcRequestBuilders
                        .put("/v1/libraryevent")
                        .content(jsonContent)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    void updateLibraryEvent_withNullLibraryEventId() throws Exception {

        Book book = Book.builder()
                .bookId(12345)
                .bookName("Apache Kafka Spring boot")
                .bookAuthor("Dilip")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        when(libraryEventProducer.sendLibraryEventsAsync_Approach2(isA(LibraryEvent.class))).thenReturn(null);

        String jsonContent = mapper.writeValueAsString(libraryEvent);
        String expectedErrorMessage = "Please pass the libraryEventId";
        mockMvc.perform(MockMvcRequestBuilders
                        .put("/v1/libraryevent")
                        .content(jsonContent)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(content().string(expectedErrorMessage));
    }
}