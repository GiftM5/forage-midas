package com.jpmc.midascore.component;


import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import org.springframework.stereotype.Component;

@Component
public class TransactionConduit {
    private final TransactionRepository transactionRepository;

    public TransactionConduit(TransactionRepository transactionRepository) {
        this.transactionRepository = transactionRepository;
    }

    public void save(TransactionRecord record) {
        transactionRepository.save(record);
    }
}

