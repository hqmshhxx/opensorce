package org.apache.hbase.repository;

import org.apache.hbase.base.AbstractHBaseRepository;
import org.apache.hbase.entity.Book;
import org.apache.hadoop.hbase.client.Connection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;


@Component
public class HBaseBookRepository extends AbstractHBaseRepository<Book> {

    @Autowired
    private Connection connection;
    @Value("${book.tableName}")
    private String tableName;
    @Value("${book.family}")
    private String family;

    public HBaseBookRepository(){
    }

    @PostConstruct
    public void init() {

    }

}
