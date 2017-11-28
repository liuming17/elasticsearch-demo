package com.elasticsearch.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;

/**
 * Created by liuming on 2017/11/12.
 */
@Configuration
@EnableElasticsearchRepositories(basePackages = "com.elasticsearch.**.repository")
public class ElasticsearchConfig {

}
