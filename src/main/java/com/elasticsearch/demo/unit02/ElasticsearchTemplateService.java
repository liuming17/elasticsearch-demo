package com.elasticsearch.demo.unit02;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Created by liuming on 2017/11/28.
 */
@Service
public class ElasticsearchTemplateService {

    private Logger logger = LoggerFactory.getLogger(getClass()) ;

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    public SearchResponse queryByTemplate(String index, String type, String queryTemplate , Map<String , Object> params){

        SearchTemplateRequestBuilder searchTemplateRequestBuilder = new SearchTemplateRequestBuilder(elasticsearchTemplate.getClient()) ;

        searchTemplateRequestBuilder.setScript(queryTemplate) ;
        searchTemplateRequestBuilder.setScriptType(ScriptType.INLINE) ;
        searchTemplateRequestBuilder.setScriptParams(params) ;
        searchTemplateRequestBuilder.setRequest(new SearchRequest(index).types(type));

        SearchResponse searchResponse = searchTemplateRequestBuilder.get().getResponse() ;

        return searchResponse ;
    }

}
