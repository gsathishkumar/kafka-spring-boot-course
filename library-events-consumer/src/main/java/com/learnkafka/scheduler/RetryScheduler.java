package com.learnkafka.scheduler;

import com.learnkafka.config.LibraryEventsConsumerConfig;
import com.learnkafka.entity.FailureRecord;
import com.learnkafka.jpa.FailureRecordsRepository;
import com.learnkafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class RetryScheduler {

    @Autowired
    private FailureRecordsRepository failureRecordsRepository;

    @Autowired
    private LibraryEventsService libraryEventsService;

    @Scheduled(fixedRate = 10000)
    public void retryFailedRecords() {
        log.info("Retrying Failed records Started");
        List<FailureRecord> allRetryStatus = failureRecordsRepository.findAllByStatus(LibraryEventsConsumerConfig.RETRY);
        log.info("Failed record count {}", allRetryStatus.size());
        allRetryStatus.forEach(failureRecord -> {
                    log.info("Retrying Failed records {}", failureRecord);

                    ConsumerRecord<Integer, String> consumerRecord = buildConsumerRecord(failureRecord);
                    try {
                        libraryEventsService.processLibraryEvent(consumerRecord);
                        failureRecord.setStatus(LibraryEventsConsumerConfig.SUCCESS);
                        failureRecordsRepository.save(failureRecord);
                    } catch (Exception e) {
                        log.error("Exception in retryFailedRecords {}", e.getMessage(), e);
                    }
                }
        );
        log.info("Retrying Failed records Completed");
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {
        return new ConsumerRecord<>(
                failureRecord.getTopic(),
                failureRecord.getPartition(),
                failureRecord.getOffset_value(),
                failureRecord.getErrorKey(),
                failureRecord.getErrorRecord()
        );
    }

}
