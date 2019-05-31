package com.elsaticsearch.test;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Description
 * @auther 冉长冬  DSL搜索
 * @create 2019-05-30 19:16
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class DSLSearch {

    @Autowired
    private RestHighLevelClient client;

    /**
     * @Description  /搜索type下的全部记录
     * @Date  2019/5/30 19:17
     **/
    @Test
    public void testSearchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest("my_course");
        searchRequest.types("doc");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询所有数据
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //source源字段过虑 是显示 需要看的数据
        searchSourceBuilder.fetchSource(new String[]{"name", "studymodel", "description"}, new String[]{});
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = searchResponse.getHits();

        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            String index = hit.getIndex();
            System.out.println("index: " + index);
            String type = hit.getType();
            System.out.println("type: " + type);
            String id = hit.getId();
            System.out.println("id: " + id);
            float score = hit.getScore();
            System.out.println("score: " + score);
            String sourceAsString = hit.getSourceAsString();
            System.out.println("sourceAsString: " + sourceAsString);
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String studymodel = (String) sourceAsMap.get("studymodel");
            String description = (String) sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }
    }

        /**
         * @Description  分页查询
         * @Date  2019/5/30 19:33
         **/
        @Test
        public void pageSearch() throws IOException {
            //需要查询的索引
            SearchRequest searchRequest = new SearchRequest("my_course");
            //需要查询索引的类型
            searchRequest.types("doc");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            //从第1条开始
            searchSourceBuilder.from(0);
            //每次一共返回两条数据
            searchSourceBuilder.size(2);
            //过滤条件 需要显示的字段
            searchSourceBuilder.fetchSource(new String[]{"name","pic"},new String[]{});
            searchRequest.source(searchSourceBuilder);
            SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = search.getHits();
            SearchHit[] hits1 = hits.getHits();
            for (SearchHit documentFields : hits1) {
                Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
                 System.out.println(sourceAsMap);
            }


        }


        /**
         * @Description  Term Query
         * Term Query为精确查询，在搜索时会整体匹配关键字，不再将关键字分词。
         * @Date  2019/5/30 19:53
         **/
        @Test
        public void termQuery() throws IOException {
            SearchRequest searchRequest = new SearchRequest("my_course");
            searchRequest.types("doc");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.termsQuery("name","spring"));

            searchSourceBuilder.fetchSource(new String[]{"name","pic"},new String[]{});
            SearchRequest source = searchRequest.source(searchSourceBuilder);
            SearchResponse search = client.search(source,RequestOptions.DEFAULT);
            SearchHits hits = search.getHits();
            SearchHit[] hits1 = hits.getHits();
            for (SearchHit documentFields : hits1) {
                Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
                 System.out.println(sourceAsMap);
            }
        }

        /**
         * @Description  根据id精确匹配
         * @Date  2019/5/30 20:00
         **/
        @Test
        public void findById() throws IOException {
            SearchRequest searchRequest = new SearchRequest("my_course");
            searchRequest.types("doc");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            String[] split = new String[]{"1","2","3"};
            List<String> idList = Arrays.asList(split);
            searchSourceBuilder.query(QueryBuilders.termsQuery("_id", idList));

            searchSourceBuilder.fetchSource(new String[]{"name","pic"},new String[]{});
            SearchRequest source = searchRequest.source(searchSourceBuilder);
            SearchResponse search = client.search(source,RequestOptions.DEFAULT);
            SearchHits hits = search.getHits();
            SearchHit[] hits1 = hits.getHits();
            for (SearchHit documentFields : hits1) {
                Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
                System.out.println(sourceAsMap);
            }
        }

        /**
         * @Description  match Query
         * match Query即全文检索，它的搜索方式是先将搜索字符串分词，再使用各各词条从索引中搜索
         * match query与Term query区别是match query在搜索前先将搜索关键字分词，再拿各各词语去索引中搜索
         * @Date  2019/5/30 20:05
         **/
        @Test
        public void matchQuery() throws IOException {
            SearchRequest searchRequest = new SearchRequest("my_course");
            searchRequest.types("doc");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchQuery("description","spring开发").operator(Operator.OR));

            searchSourceBuilder.fetchSource(new String[]{"name","price","description"},new String[]{});
            searchRequest.source(searchSourceBuilder);

            SearchResponse search = client.search(searchRequest,RequestOptions.DEFAULT);

            SearchHits hits = search.getHits();

            SearchHit[] hits1 = hits.getHits();

            for (SearchHit documentFields : hits1) {
                Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();

                 System.out.println(sourceAsMap);

            }

        }

        /**
         * @Author 冉长冬
         * @Description  minimum_should_match": "80% 表示，三个词在文档的匹配占比为80%，即3*0.8=2.4，向上取整得2，表
         * 示至少有两个词在文档中要匹配成功。
         * @Date  2019/5/30 20:29
         **/
        @Test
        public void minimumShouldmatchQuery() throws IOException {
            SearchRequest searchRequest = new SearchRequest("my_course");
            searchRequest.types("doc");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            searchSourceBuilder.fetchSource(new String[]{"name","price","description"},new String[]{});

            searchSourceBuilder.query(QueryBuilders.multiMatchQuery("spring框架", "name", "description").minimumShouldMatch("50%").field("name",10));


            searchRequest.source(searchSourceBuilder);

            SearchResponse search = client.search(searchRequest,RequestOptions.DEFAULT);

            SearchHits hits = search.getHits();

            SearchHit[] hits1 = hits.getHits();

            for (SearchHit documentFields : hits1) {
                Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();

                System.out.println(sourceAsMap);

            }

        }


        /**
         * @Description  布尔查询
         * 布尔查询对应于Lucene的BooleanQuery查询，实现将多个查询组合起来
         *
         * 三个参数：
         * must：文档必须匹配must所包括的查询条件，相当于 “AND”
         * should：文档应该匹配should所包括的查询条件其中的一个或多个，相当于 "OR"
         * must_not：文档不能匹配must_not所包括的该查询条件，相当于“NOT”分别使用must、should、must_not测试下边的查询
         * @Date  2019/5/30 20:41
         **/
        @Test
        public void booleanSearch() throws IOException {
            //创建搜索请求对象
            SearchRequest searchRequest= new SearchRequest("my_course");
            searchRequest.types("doc");
            //创建搜索源配置对象
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.fetchSource(new String[]{"name","pic","studymodel"},new String[]{});
            MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("spring框架", "name", "description")
                    .minimumShouldMatch("50%").field("name", 10);
            TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("studymodel", "201001");

            //布尔查询
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            boolQueryBuilder.must(multiMatchQueryBuilder);
            boolQueryBuilder.must(termsQueryBuilder);

            searchSourceBuilder.query(boolQueryBuilder);

            SearchRequest source = searchRequest.source(searchSourceBuilder);
            SearchResponse search = client.search(source,RequestOptions.DEFAULT);
            SearchHits hits = search.getHits();

            SearchHit[] hits1 = hits.getHits();

            for (SearchHit documentFields : hits1) {
                Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
                 System.out.println(sourceAsMap);
            }


        }


        /**
         * @Author 冉长冬
         * @Description  过滤器
         * @Date  2019/5/30 20:53
         * @Param
         * @return
         **/

        @Test
        public void filterSearch() throws IOException {
            SearchRequest searchRequest = new SearchRequest("my_course");
            searchRequest.types("doc");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            //source源字段过虑
            searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","description"},
                    new String[]{});
            searchRequest.source(searchSourceBuilder);
            //匹配关键字
            MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("spring框架", "name", "description").minimumShouldMatch("50%")
                    .field("name", 10);

            //设置匹配占比
            //提升另个字段的Boost值
            searchSourceBuilder.query(multiMatchQueryBuilder);
            //布尔查询
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            boolQueryBuilder.must(searchSourceBuilder.query());
            //过虑
            boolQueryBuilder.filter(QueryBuilders.termQuery("studymodel", "201001"));
            boolQueryBuilder.filter(QueryBuilders.rangeQuery("pricec").gte(0).lte(100));

            SearchResponse search = client.search(searchRequest,RequestOptions.DEFAULT);
            SearchHits hits = search.getHits();
            SearchHit[] hits1 = hits.getHits();

            for (SearchHit documentFields : hits1) {
                Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
                 System.out.println(sourceAsMap);
            }


        }

        /**
         * @Description  排序
         * @Date  2019/5/30 23:07
         **/
        @Test
        public void sortSearch() throws IOException {
            SearchRequest searchRequest = new SearchRequest("my_course");
            searchRequest.types("doc");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            //source源字段过虑
            searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","description"},
                    new String[]{});

            //布尔查询
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(0).lte(40));
            searchSourceBuilder.query(boolQueryBuilder);

            //排序
            searchSourceBuilder.sort(new FieldSortBuilder("studymodel").order(SortOrder.DESC));

            searchRequest.source(searchSourceBuilder);

            SearchResponse search = client.search(searchRequest,RequestOptions.DEFAULT);

            SearchHits hits = search.getHits();

            SearchHit[] hits1 = hits.getHits();
            for (SearchHit documentFields : hits1) {
                Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();

                 System.out.println(sourceAsMap);
            }

        }

        /**
         * @Description 高亮显示
         * @Date  2019/5/31 8:50
         **/
        @Test
        public void  HighlightSearch() throws IOException {
            SearchRequest searchRequest = new SearchRequest("my_course");
            searchRequest.types("doc");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            //source源字段过虑
            searchSourceBuilder.fetchSource(new String[]{"name","price","studymodel","description"},new String[]{});

            //匹配关键字
            MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("开发", "name", "description");
            searchSourceBuilder.query(multiMatchQueryBuilder);

            //布尔查询
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            //过虑
            boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(0).lte(80));
            boolQueryBuilder.must(searchSourceBuilder.query());

            //排序
            searchSourceBuilder.sort(new FieldSortBuilder("price").order(SortOrder.DESC));
            //高亮设置

            HighlightBuilder highlightBuilder = new HighlightBuilder();
            highlightBuilder.preTags("<font style='color:red;'>"); //前缀
            highlightBuilder.postTags("</font>"); //后缀
            //设置高亮显示的字段
            highlightBuilder.fields().add(new HighlightBuilder.Field("name"));
            highlightBuilder.fields().add(new HighlightBuilder.Field("description"));

            searchSourceBuilder.highlighter(highlightBuilder);

            SearchRequest source = searchRequest.source(searchSourceBuilder);
            SearchResponse search = client.search(source,RequestOptions.DEFAULT);

            SearchHits hits = search.getHits();

            SearchHit[] hits1 = hits.getHits();

            for (SearchHit documentFields : hits1) {
                Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
                Double price = (Double) sourceAsMap.get("price");
                System.out.println(price);
                Map<String, HighlightField> highlightFields = documentFields.getHighlightFields();
               if(highlightFields!= null){
                   HighlightField name1 = highlightFields.get("name");
                   HighlightField descriptions = highlightFields.get("description");
                   if(name1 != null){
                       Text[] fragments = name1.getFragments();
                       StringBuffer stringBuffer = new StringBuffer();
                       for (Text fragment : fragments) {
                           stringBuffer.append(fragment.string());
                       }
                       String name = stringBuffer.toString();

                       System.out.println(name);
                   }
                   if(descriptions!=null){
                       Text[] fragments = descriptions.getFragments();
                       StringBuffer stringBuffer = new StringBuffer();
                       if(fragments!=null){
                           for (Text fragment : fragments) {
                               stringBuffer.append(fragment.string());
                           }
                           String description = stringBuffer.toString();
                           System.out.println(description);
                       }
                   }

               }
                System.out.println("==========================");
            }



        }




}
