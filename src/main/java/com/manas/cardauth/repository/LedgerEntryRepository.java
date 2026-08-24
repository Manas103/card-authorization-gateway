package com.manas.cardauth.repository;

import com.manas.cardauth.model.LedgerEntryEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface LedgerEntryRepository extends JpaRepository<LedgerEntryEntity, Long> {
    Optional<LedgerEntryEntity> findByTraceNumber(String traceNumber);
}
