package com.elasticsearch.demo.unit01;

import com.elasticsearch.demo.vo.Employee;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class ElasticsearchCurdService {

    private Logger logger = LoggerFactory.getLogger(getClass()) ;

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    /**
     * 添加demo
     * @param index
     * @param type
     * @param employee
     */
    public void add(String index, String type, Employee employee){
        ObjectMapper objectMapper = new ObjectMapper() ;
        String json = null;
        try {
            json = objectMapper.writeValueAsString(employee);
            logger.info("添加document:{}" , json);
            elasticsearchTemplate.getClient().prepareIndex().setIndex(index).setType(type)
                    .setId(employee.getId().toString()).setSource(json , XContentType.JSON).get() ;
        } catch (JsonProcessingException e) {
            logger.error("转换json异常" , e);
        }
    }

    /**
     * 根据ID获取文档
     * @param index
     * @param type
     * @param id
     * @return
     */
    public String getById(String index, String type, String id){
        GetResponse response = elasticsearchTemplate.getClient()
                .prepareGet(index, type, id).setOperationThreaded(false).get();
        String result = response.getSourceAsString() ;
        logger.info("根据id:{}查找文档,结果:{}", id, result);
        return result ;
    }

    /**
     * 获取所有 默认只返回前10条结果
     * @param index
     * @param type
     * @return
     */
    public List<String> getAll(String index, String type){
        SearchResponse searchResponse = elasticsearchTemplate.getClient()
                .prepareSearch(index).setTypes(type).get() ;

        SearchHits hits = searchResponse.getHits() ;

        return covertResult(hits) ;
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
                logger.info("hit score : {}" , hit.getScore());
                source = hit.getSourceAsString() ;
                logger.info("search : {}", source);
                result.add(source) ;
            }
        }
        return result ;
    }

    /**
     * query-string 查询方式
     * @param index
     * @param type
     * @param queryStr
     * @return
     */
    public List<String> getByQueryString(String index, String type, String queryStr){

        SearchResponse searchResponse = elasticsearchTemplate.getClient()
                .prepareSearch(index).setTypes(type).setQuery(QueryBuilders.queryStringQuery(queryStr)).get() ;

        SearchHits hits = searchResponse.getHits() ;

        return covertResult(hits) ;
    }

    /**
     * 简单DSL 搜索
     * @param index
     * @param type
     * @param key
     * @param value
     * @return
     */
    public List<String> getByQueryMatchDsl(String index, String type, String key, Object value){
        QueryBuilder queryBuilder = QueryBuilders.matchQuery(key , value) ;

        SearchResponse searchResponse = elasticsearchTemplate.getClient()
                .prepareSearch(index).setTypes(type).setQuery(queryBuilder).get() ;

        SearchHits hits = searchResponse.getHits() ;

        return covertResult(hits) ;
    }

    /**
     * 复杂的搜索  match and filter
     * @param index
     * @param type
     * @param key
     * @param value
     * @param filterKey
     * @param gtValue
     * @return
     */
    public List<String> getByMatchAndFilter(String index, String type, String key, Object value, String filterKey , Object gtValue){
        QueryBuilder queryBuilder = QueryBuilders.matchQuery(key , value) ;

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(queryBuilder) ;
        boolQueryBuilder.filter(QueryBuilders.rangeQuery(filterKey).gt(gtValue)) ;

        SearchResponse searchResponse = elasticsearchTemplate.getClient()
                .prepareSearch(index).setTypes(type).setQuery(boolQueryBuilder).get() ;

        SearchHits hits = searchResponse.getHits() ;

        return covertResult(hits) ;
    }

    /**
     * 短语搜索
     * @param index
     * @param type
     * @param key
     * @param value
     * @return
     */
    public List<String> getByQueryMatchPhraseDsl(String index, String type, String key, Object value){
        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery(key , value) ;

        SearchResponse searchResponse = elasticsearchTemplate.getClient()
                .prepareSearch(index).setTypes(type).setQuery(queryBuilder).get() ;

        SearchHits hits = searchResponse.getHits() ;

        return covertResult(hits) ;
    }


    /**
     * 高亮显示关键字
     * @param index
     * @param type
     * @param key
     * @param value
     * @return
     */
    public List<String> getByQueryMatchPhraseAndHighlightDsl(String index, String type, String key, Object value){
        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery(key , value) ;

        HighlightBuilder highlightBuilder = new HighlightBuilder().field(key).requireFieldMatch(true) ;
        highlightBuilder.preTags("<span style='color:red'>") ;
        highlightBuilder.postTags("</span>") ;

        SearchResponse searchResponse = elasticsearchTemplate.getClient()
                .prepareSearch(index).setTypes(type).setQuery(queryBuilder).highlighter(highlightBuilder).get() ;

        SearchHits hits = searchResponse.getHits() ;

        if(hits != null && hits.getHits() != null){
            for(SearchHit hit : hits.getHits()){
                if(hit.getHighlightFields() != null){
                    //获取对应的高亮域
                    Map<String, HighlightField> result = hit.getHighlightFields() ;
                    //从设定的高亮域中取得指定域
                    HighlightField titleField = result.get(key);

                    if(titleField != null){
                        //取得定义的高亮标签
                        Text[] titleTexts =  titleField.fragments();

                        for(Text text : titleTexts){
                            logger.info("HighlightField key : {} , value : {} " , key , text);
                        }
                    }
                }
            }
        }

        return covertResult(hits) ;
    }


    /**
     * 分析
     * @param index
     * @param type
     * @param key
     * @return
     */
    public void group(String index, String type, String key){
        AggregationBuilder aggregationBuilder = AggregationBuilders.terms("all_interests").field(key);

        SearchResponse searchResponse = elasticsearchTemplate.getClient()
                .prepareSearch(index).setTypes(type).addAggregation(aggregationBuilder).get() ;

        Terms interests = searchResponse.getAggregations().get("all_interests") ;

        if(interests != null && interests.getBuckets() != null){
            for(Terms.Bucket bucket : interests.getBuckets()){
                logger.info("key : {} , doc_count : {} " , bucket.getKey() , bucket.getDocCount());
            }
        }
    }

    public void groupAndQuery(String index, String type, String key, String queryKey , Object queryValue){
        AggregationBuilder aggregationBuilder = AggregationBuilders.terms("all_interests").field(key);

        QueryBuilder queryBuilder = QueryBuilders.matchQuery(queryKey , queryValue) ;

        SearchResponse searchResponse = elasticsearchTemplate.getClient()
                .prepareSearch(index).setTypes(type).setQuery(queryBuilder).addAggregation(aggregationBuilder).get() ;

        Terms interests = searchResponse.getAggregations().get("all_interests") ;

        if(interests != null && interests.getBuckets() != null){
            for(Terms.Bucket bucket : interests.getBuckets()){
                logger.info("key : {} , doc_count : {} " , bucket.getKey() , bucket.getDocCount());
            }
        }
    }

    public void groupAndAvg(String index, String type, String key, String avgKey){
        AggregationBuilder aggregationBuilder = AggregationBuilders.terms("all_interests").field(key);
        AggregationBuilder avgAggreationBuilder = AggregationBuilders.avg("avg_age").field(avgKey) ;

        aggregationBuilder.subAggregation(avgAggreationBuilder) ;

        SearchResponse searchResponse = elasticsearchTemplate.getClient()
                .prepareSearch(index).setTypes(type).addAggregation(aggregationBuilder).get() ;

        Terms interests = searchResponse.getAggregations().get("all_interests") ;

        if(interests != null && interests.getBuckets() != null){
            for(Terms.Bucket bucket : interests.getBuckets()){
                InternalAvg internalAvg = bucket.getAggregations().get("avg_age") ;
                logger.info("key : {} , doc_count : {}  , avg : {} " , bucket.getKey() , bucket.getDocCount() , internalAvg.getValue());
            }
        }
    }

}
