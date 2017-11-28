package com.elasticsearch.demo.repository;

import com.elasticsearch.demo.vo.Employee;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

/**
 * spring-data-elasticsearch 另外一种实现
 * Created by liuming on 2017/11/12.
 */
public interface EmployeeRepository extends ElasticsearchRepository<Employee , Long> {

}
