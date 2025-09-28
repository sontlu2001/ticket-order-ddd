package com.xxxx.ddd.application.service.ticket;

import com.xxxx.ddd.domain.model.entity.TicketDetail;

public interface TicketDetailAppService {

    TicketDetail getTicketDetailById(Long ticketId); // should convert to TickDetailDTO by Application Module
}
