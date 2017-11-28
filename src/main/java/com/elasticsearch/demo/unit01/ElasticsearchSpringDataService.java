package com.elasticsearch.demo.unit01;

import com.elasticsearch.demo.vo.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.DefaultEntityMapper;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.aggregation.impl.AggregatedPageImpl;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by liuming on 2017/11/12.
 */
@Service
public class ElasticsearchSpringDataService {

    private Logger logger = LoggerFactory.getLogger(getClass()) ;

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    /**
     *  elasticsearchTemplate 实现
     * @param employee
     */
    public void add(Employee employee){
        IndexQuery indexQuery = new IndexQueryBuilder().withId(employee.getId().toString()).withObject(employee).build();
        elasticsearchTemplate.index(indexQuery) ;
    }

    /**
     * 根据ID查找
     * @param id
     * @param clazz
     * @param <T>
     * @return
     */
    public <T> T getById(String id , Class<T> clazz){
        GetQuery getQuery = new GetQuery() ;
        getQuery.setId(id);
        return elasticsearchTemplate.queryForObject(getQuery , clazz) ;
    }

    /**
     * 查询所有
     * @param clazz
     * @param <T>
     * @return
     */
    public <T> List<T> getAll(Class<T> clazz){
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.matchAllQuery()).build();
        return elasticsearchTemplate.queryForList(searchQuery , clazz) ;
    }

    /**
     * querystring查询方式
     * example:  last_name:Smith
     * @param query
     * @param clazz
     * @param <T>
     * @return
     */
    public <T> List<T> getByQueryString(String query, Class<T> clazz){
        if(StringUtils.isBlank(query)){
            return null ;
        }
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(new QueryStringQueryBuilder(query)).build();
        return elasticsearchTemplate.queryForList(searchQuery , clazz) ;
    }

    /**
     *
     * @param key
     * @param value
     * @param clazz
     * @param <T>
     * @return
     */
    public <T> List<T> getByQueryMatchDsl(String key , Object value , Class<T> clazz){
        QueryBuilder queryBuilder = QueryBuilders.matchQuery(key , value) ;
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(queryBuilder).build();
        return elasticsearchTemplate.queryForList(searchQuery , clazz) ;
    }

    /**
     *
     * @param key
     * @param value
     * @param filterKey
     * @param gtValue
     * @param clazz
     * @param <T>
     * @return
     */
    public <T> List<T> getByMatchAndFilter(String key, Object value, String filterKey, Object gtValue, Class<T> clazz){
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder() ;

        QueryBuilder queryBuilder = QueryBuilders.matchQuery(key , value) ;
        boolQueryBuilder.must(queryBuilder) ;

        QueryBuilder filterBuilder = QueryBuilders.rangeQuery(filterKey).gt(gtValue) ;
        boolQueryBuilder.filter(filterBuilder) ;

        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(boolQueryBuilder).build();
        return elasticsearchTemplate.queryForList(searchQuery , clazz) ;
    }

    /**
     * 短语搜索
     * @param key
     * @param value
     * @return
     */
    public <T> List<T> getByQueryMatchPhraseDsl(String key, Object value, Class<T> clazz){
        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery(key , value) ;
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(queryBuilder).build();
        return elasticsearchTemplate.queryForList(searchQuery , clazz) ;
    }

    /**
     * 短语搜索  高亮设置
     * @param key
     * @param value
     * @param clazz
     * @param <T>
     * @return
     * @throws Exception
     */
    public <T> List<T> getByQueryMatchPhraseAndHighlightDsl(String key, Object value, Class<T> clazz) throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery(key , value) ;

        HighlightBuilder.Field field = new HighlightBuilder.Field(key).preTags("<span style='color:red'>").postTags("</span>") ;

        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(queryBuilder).withHighlightFields(field).build();
        Page<T> page = elasticsearchTemplate.queryForPage(searchQuery, clazz, new SearchResultMapper() {
            @Override
            public <T> AggregatedPage<T> mapResults(SearchResponse response, Class<T> clazz, Pageable pageable) {
                List<T> list = new ArrayList<>() ;
                if(response.getHits() != null && response.getHits().getHits() != null){
                    for(SearchHit hit : response.getHits().getHits()){
                        if(hit.getHighlightFields() != null){
                            Map<String, Object> entityMap = hit.getSource();

                            //获取对应的高亮域
                            Map<String, HighlightField> result = hit.getHighlightFields() ;
                            //从设定的高亮域中取得指定域
                            HighlightField titleField = result.get(key);
                            if(titleField != null){
                                //取得定义的高亮标签
                                Text[] titleTexts =  titleField.fragments();
                                for(Text text : titleTexts){
                                    logger.info("HighlightField key : {} , value : {} " , text , text.toString());
                                    entityMap.put(key , text.toString()) ;
                                }
                            }
                            DefaultEntityMapper defaultEntityMapper = new DefaultEntityMapper() ;
                            try {
                                ObjectMapper objectMapper = new ObjectMapper() ;
                                T bean = defaultEntityMapper.mapToObject(objectMapper.writeValueAsString(entityMap), clazz) ;
                                list.add(bean) ;
                            } catch (IOException e) {
                                logger.error("对象转换异常: {}", hit.getSourceAsString() ,e);
                            }
                        }
                    }
                }
                return new AggregatedPageImpl<T>(list) ;
            }
        }) ;
        return page.getContent() ;
    }


    /**
     *
     * @param index
     * @param type
     * @param key
     * @param <T>
     */
    public <T> void group(String index, String type, String key){
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("all_interests").field(key) ;
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withIndices(index).withTypes(type).addAggregation(aggregationBuilder).build();

        Aggregations aggregations = elasticsearchTemplate.query(searchQuery, new ResultsExtractor<Aggregations>() {
            @Override
            public Aggregations extract(SearchResponse response) {
                return response.getAggregations();
            }
        });

        Terms interests = aggregations.get("all_interests") ;
        if(interests != null && interests.getBuckets() != null){
            for(Terms.Bucket bucket : interests.getBuckets()){
                logger.info("key : {} , doc_count : {} " , bucket.getKey() , bucket.getDocCount());
            }
        }
    }

    /**
     *
     * @param index
     * @param type
     * @param key
     * @param queryKey
     * @param queryValue
     */
    public void groupAndQuery(String index, String type, String key, String queryKey , Object queryValue){
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("all_interests").field(key) ;

        QueryBuilder queryBuilder = QueryBuilders.matchQuery(queryKey , queryValue) ;
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withIndices(index).withTypes(type)
                .withQuery(queryBuilder).addAggregation(aggregationBuilder).build();

        Aggregations aggregations = elasticsearchTemplate.query(searchQuery, new ResultsExtractor<Aggregations>() {
            @Override
            public Aggregations extract(SearchResponse response) {
                return response.getAggregations();
            }
        });

        Terms interests = aggregations.get("all_interests") ;
        if(interests != null && interests.getBuckets() != null){
            for(Terms.Bucket bucket : interests.getBuckets()){
                logger.info("key : {} , doc_count : {} " , bucket.getKey() , bucket.getDocCount());
            }
        }
    }

    /**
     *
     * @param index
     * @param type
     * @param key
     * @param avgKey
     */
    public void groupAndAvg(String index, String type, String key, String avgKey){
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("all_interests").field(key) ;
        AvgAggregationBuilder avgAggregationBuilder = AggregationBuilders.avg("avg_age").field(avgKey) ;
        aggregationBuilder.subAggregation(avgAggregationBuilder) ;

        SearchQuery searchQuery = new NativeSearchQueryBuilder().withIndices(index).withTypes(type)
                .addAggregation(aggregationBuilder).build();

        Aggregations aggregations = elasticsearchTemplate.query(searchQuery, new ResultsExtractor<Aggregations>() {
            @Override
            public Aggregations extract(SearchResponse response) {
                return response.getAggregations();
            }
        });

        Terms interests = aggregations.get("all_interests") ;
        if(interests != null && interests.getBuckets() != null){
            for(Terms.Bucket bucket : interests.getBuckets()){
                InternalAvg internalAvg = bucket.getAggregations().get("avg_age") ;
                logger.info("key : {} , doc_count : {}  , avg : {} " , bucket.getKey() , bucket.getDocCount() , internalAvg.getValue());
            }
        }
    }
}
