01章节
    两种方式实现 原生java api实现 + spring-data-elasticsearch实现，
    为了比对数据 spring-data-elasticsearch 使用的index 、type分别为megacorp_spring、employee_spring
    Repository 方式 存在问题，暂未解决，后续再看！

    包含以下基本处理：
    1、添加文档
    PUT	/megacorp/employee/1
    {
       "first_name":"John",
       "last_name":"Smith",
       "age":25,
       "about":"I love to go rock climbing",
       "interests":["sports","music"]
    }

    2、获取文档
    GET	/megacorp/employee/1

    3、简单搜索之查询全部
    GET	/megacorp/employee/_search

    4、简单搜索之query string
    GET	/megacorp/employee/_search?q=last_name:Smith

    5、简单搜索之DSL
    POST /megacorp/employee/_search
    {"query":{"match":{"last_name":"Smith"}}}

    6、复杂搜索之filter
    POST /megacorp/employee/_search
    {
        "query":{
            "filtered":{
                "filter":{"range":{"age":{"gt":30}}},
                "query":{"match":{"last_name":"smith"}}
            }
        }
    }

    7、全文搜索 部分包含
    POST /megacorp/employee/_search
    {"query": {"match": {"about": "rock	climbing"}}}

    8、短语搜索 同时包含
    POST /megacorp/employee/_search
    {"query": {"match_phrase": {"about": "rock	climbing"}}}

    9、高亮搜索 默认是使用<em></em>标识 也可以指定包围 pre_tags 、post_tags
    POST /megacorp/employee/_search
    {"query":{"match_phrase":{"about":"rock climbing"}},"highlight":{"fields":{"about":{}},"pre_tags":"<span style='color:red'>","post_tags":"</span>"}}

    10、分析，使用默认会出现Fielddata is disabled on text fields by default错误，需配置mapping开启
    5.x后对排序，聚合这些操作用单独的数据结构(fielddata)缓存到内存里了，需要单独开启
    PUT megacorp/_mapping/employee/
    {"properties": {"interests": { "type": "text","fielddata": true}}}

    POST /megacorp/employee/_search
    {"aggs": {"all_interests": {"terms": { "field": "interests" }}}}

    11、查询分析
    POST /megacorp/employee/_search
    {
      "query": {"match": {"last_name": "smith"}},
      "aggs": {"all_interests": {"terms": {"field": "interests"}}}
    }

    12、分级汇总
    POST /megacorp/employee/_search
    {"aggs":{"all_interests" : {"terms":{ "field" : "interests" },"aggs" : {"avg_age" : {"avg" : { "field" : "age" }}}}}}