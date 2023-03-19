package com.learnkafka.service;

import com.learnkafka.entity.FailureRecord;
import com.learnkafka.jpa.FailureRecordsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FailureRecordService {

    private final FailureRecordsRepository failureRecordsRepository;

    public FailureRecordService(FailureRecordsRepository failureRecordsRepository) {
        this.failureRecordsRepository = failureRecordsRepository;
    }

    public void saveFailedRecord(ConsumerRecord<Integer, String> consumerRecord, Exception e, String status) {
        FailureRecord failureRecord = FailureRecord.builder()
                .topic(consumerRecord.topic())
                .errorKey(consumerRecord.key())
                .errorRecord(consumerRecord.value())
                .partition(consumerRecord.partition())
                .offset_value(consumerRecord.offset())
                .exception(e.getCause().getMessage())
                .status(status)
                .build();

        failureRecordsRepository.save(failureRecord);
    }
}
