package com.xxxx.ddd.infrastructure.persistence.repository;

import com.xxxx.ddd.domain.model.entity.TicketDetail;
import com.xxxx.ddd.domain.respository.TicketDetailRepository;
import com.xxxx.ddd.infrastructure.persistence.mapper.TicketDetailJPAMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class TicketDetailInfrasRepositoryImpl implements TicketDetailRepository {

    // CALL JPA MAPPER

    @Autowired
    private TicketDetailJPAMapper ticketDetailJPAMapper;

    @Override
    public Optional<TicketDetail> findById(Long id) {
        log.info("Implement Infrastructure : {}", id);
        return ticketDetailJPAMapper.findById(id);
    }
}
