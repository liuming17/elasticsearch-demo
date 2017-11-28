package com.elasticsearch.demo.unit02;

import com.elasticsearch.demo.ElasticsearchApplication;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by liuming on 2017/11/28.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes= ElasticsearchApplication.class)
public class ElasticsearchTemplateServiceTest {

    private final Logger logger = LoggerFactory.getLogger(getClass()) ;

    final String index = "megacorp" ;
    final String type = "employee" ;

    @Autowired
    private ElasticsearchTemplateService elasticsearchTemplateService ;

    @Test
    public void queryByTemplate() throws Exception {
//        String queryTempalte = "{\"query\":{\"match\":{\"last_name\":\"{{last_name}}\"}}}" ;
        String queryTempalte = "{\"query\": {\"match\": {\"first_ame\": \"{{last_name}}\"}},\"aggs\": {\"all_interests\": {\"terms\": {\"field\": \"last_name\"}}}}" ;

        Map<String , Object> params = new HashMap<>() ;
        params.put("last_name" , "John") ;

        SearchResponse searchResponse = elasticsearchTemplateService.queryByTemplate(index, type, queryTempalte, params) ;

        Terms interests = searchResponse.getAggregations().get("all_interests") ;

        if(interests != null && interests.getBuckets() != null){
            for(Terms.Bucket bucket : interests.getBuckets()){
                logger.info("key : {} , doc_count : {} " , bucket.getKey() , bucket.getDocCount());
            }
        }

        //SearchHits hits = searchResponse.getHits() ;
        //covertResult(hits) ;
    }

    /**
     * 结果转换
     * @param hits
     * @return
     */
    private List<String> covertResult(SearchHits hits){
        List<String> result = null ;
        if(hits.getHits() != null){
            result = new ArrayList<>() ;
            String source = null ;
            for(SearchHit hit : hits.getHits()){
                source = hit.getSourceAsString() ;
                logger.info("search : {}", source);
                result.add(source) ;
            }
        }
        return result ;
    }

}