package com.xxxx.ddd.infrastructure.persistence.repository;

import com.xxxx.ddd.domain.respository.HiDomainRepository;
import org.springframework.stereotype.Service;

@Service
public class HiInfrasRepositoryImpl implements HiDomainRepository {
    @Override
    public String sayHi(String who) {
        return "Hi Infrastructure";
    }
}
