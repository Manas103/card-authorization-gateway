package com.manas.cardauth.service;

import com.manas.cardauth.engine.AuthorizationLedgerEngine;
import com.manas.cardauth.engine.BalanceSnapshot;
import com.manas.cardauth.engine.LedgerOutcome;
import com.manas.cardauth.model.AccountBalanceEntity;
import com.manas.cardauth.model.CardMessage;
import com.manas.cardauth.model.LedgerEntryEntity;
import com.manas.cardauth.repository.AccountBalanceRepository;
import com.manas.cardauth.repository.LedgerEntryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Wires the pure {@link AuthorizationLedgerEngine} to persistence. The
 * engine itself has no JPA or Kafka dependency (it is unit-testable and
 * reusable in the benchmark runner without a Spring context or a
 * database); this service is the only place a hold, a release or a
 * clearing becomes a database row.
 */
@Service
public class CardAuthorizationService {

    private static final Logger log = LoggerFactory.getLogger(CardAuthorizationService.class);

    private final AuthorizationLedgerEngine engine;
    private final AccountBalanceRepository accountBalanceRepository;
    private final LedgerEntryRepository ledgerEntryRepository;

    public CardAuthorizationService(AuthorizationLedgerEngine engine,
                                     AccountBalanceRepository accountBalanceRepository,
                                     LedgerEntryRepository ledgerEntryRepository) {
        this.engine = engine;
        this.accountBalanceRepository = accountBalanceRepository;
        this.ledgerEntryRepository = ledgerEntryRepository;
    }

    @Transactional
    public LedgerOutcome process(CardMessage message) {
        LedgerOutcome outcome = engine.ingest(message);
        switch (outcome.type()) {
            case HELD -> persistBalance(outcome.accountId());
            case RELEASED, CLEARED -> {
                persistBalance(outcome.accountId());
                persistLedgerEntry(outcome);
            }
            case BUFFERED -> log.debug("terminal event buffered, awaiting authorization: traceNumber={}", message.traceNumber());
            case DUPLICATE -> log.debug("duplicate (messageId, traceNumber) ignored: messageId={} traceNumber={}",
                    message.messageId(), message.traceNumber());
        }
        return outcome;
    }

    private void persistBalance(String accountId) {
        BalanceSnapshot snapshot = engine.balanceSnapshotFor(accountId);
        AccountBalanceEntity entity = accountBalanceRepository.findByAccountId(accountId)
                .orElseGet(() -> new AccountBalanceEntity(accountId, engine.openingBalanceFor(accountId),
                        snapshot.availableBalance(), snapshot.heldAmount(), snapshot.postedBalance(), Instant.now()));
        entity.setAvailableBalance(snapshot.availableBalance());
        entity.setHeldAmount(snapshot.heldAmount());
        entity.setPostedBalance(snapshot.postedBalance());
        entity.setUpdatedAt(Instant.now());
        accountBalanceRepository.save(entity);
    }

    private void persistLedgerEntry(LedgerOutcome outcome) {
        var resolution = outcome.resolution();
        ledgerEntryRepository.save(new LedgerEntryEntity(
                resolution.traceNumber(), resolution.accountId(), resolution.effect(),
                resolution.amount(), resolution.resolvingMessageId(), Instant.now()));
    }
}
