package com.manas.cardauth.repository;

import com.manas.cardauth.model.AccountBalanceEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface AccountBalanceRepository extends JpaRepository<AccountBalanceEntity, Long> {
    Optional<AccountBalanceEntity> findByAccountId(String accountId);
}
