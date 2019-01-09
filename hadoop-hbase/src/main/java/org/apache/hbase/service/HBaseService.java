package org.apache.hbase.service;

import org.apache.hbase.entity.Book;
import org.apache.hbase.repository.HBaseBookRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class HBaseService {

    private static final Logger logger = LoggerFactory.getLogger(HBaseService.class);
    @Autowired
    private HBaseBookRepository hBaseBookRepository;


    @PostConstruct
    public void init(){
        try {
            Book book = hBaseBookRepository.get("rowkey");
            logger.info(book.toString());
        }catch (Exception e){
            logger.error(e.getMessage());
        }

    }
}
