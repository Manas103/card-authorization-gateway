package com.manas.cardauth.config;

import com.manas.cardauth.engine.AuthorizationLedgerEngine;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds the {@link AuthorizationLedgerEngine} with its opening account
 * balances. A production deployment would load real account balances
 * from PostgreSQL at startup; this demo config seeds a fixed synthetic
 * account set instead (documented as a simplification in the README),
 * since standing up real account master data is out of scope for what
 * this project measures.
 */
@Configuration
public class EngineConfig {

    @Bean
    public AuthorizationLedgerEngine authorizationLedgerEngine(
            @Value("${cardauth.accounts.count:50}") int accountCount,
            @Value("${cardauth.accounts.opening-balance:1000000.00}") String openingBalance) {
        Map<String, BigDecimal> openingBalances = new LinkedHashMap<>();
        BigDecimal balance = new BigDecimal(openingBalance);
        for (int a = 0; a < accountCount; a++) {
            openingBalances.put("ACCT-%06d".formatted(a), balance);
        }
        return new AuthorizationLedgerEngine(openingBalances);
    }
}
