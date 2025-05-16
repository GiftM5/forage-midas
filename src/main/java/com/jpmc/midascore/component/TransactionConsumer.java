package com.jpmc.midascore.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionConsumer {

    private final TransactionConduit conduit;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TransactionConsumer(TransactionConduit conduit) {
        this.conduit = conduit;
    }

    @KafkaListener(topics = "transactions", groupId = "midas")
    public void listen(String message) {
        try {
            Transaction transaction = objectMapper.readValue(message, Transaction.class);
            TransactionRecord record = new TransactionRecord(
                    transaction.getSenderId(),
                    transaction.getRecipientId(),
                    transaction.getAmount()
            );
            conduit.save(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
