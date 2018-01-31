package com.elasticsearch.demo.unit02;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Created by liuming on 2017/12/6.
 */
@Service
public class ElasticsearchScanImportService {

    private Logger logger = LoggerFactory.getLogger(getClass()) ;

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    /**
     * 全量从旧索引导入新索引
     * @param oldIndex
     * @param newIndex
     * @param size 批量提交size
     */
    public void importAll(String oldIndex , String newIndex , int size){

        Client client = elasticsearchTemplate.getClient() ;
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(oldIndex) ;

        SearchResponse response = searchRequestBuilder
                .setSize(size).setSearchType(SearchType.DEFAULT)
                .setScroll(TimeValue.timeValueMinutes(8)).execute().actionGet() ;

        //第一次返回第一页内容 + 总数
        Long sum = 0L ;

        Long count = response.getHits().getTotalHits() ;

        logger.info("doc count : {} " , count);

        SearchHit[] searchHists = response.getHits().getHits();
        doImport(searchHists, newIndex) ;

        sum += searchHists.length ;

        for(int i = 0; sum<count; i++){
            logger.info("response.getScrollId() {} " , response.getScrollId());
            response = client.prepareSearchScroll(response.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(8))
                    .execute().actionGet();
            sum += response.getHits().getHits().length;

            searchHists = response.getHits().getHits();
            doImport(searchHists, newIndex);
        }
    }

    /**
     * 导入逻辑处理
     * @param searchHists
     * @param newIndex
     */
    private void doImport(SearchHit[] searchHists, String newIndex){
        if(searchHists == null || searchHists.length == 0){
            return ;
        }
        Client client = elasticsearchTemplate.getClient() ;
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        Map<String, Object> source = null;
        IndexRequest indexRequest = null ;
        for (SearchHit hit : searchHists) {
            indexRequest = new IndexRequest(newIndex, hit.getType(), hit.getId()) ;
            indexRequest.source(hit.getSourceAsString(), XContentType.JSON) ;

            bulkRequest.add(indexRequest) ;
        }
        bulkRequest.execute().actionGet() ;
        logger.info("import source length : {} " , searchHists.length);
    }
}
