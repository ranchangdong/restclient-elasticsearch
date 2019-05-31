package com.elsaticsearch.test;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @auther 冉长冬  RestClient搜索
 * @create 2019-05-30 16:06
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class TestIndex {

    @Autowired
    private RestHighLevelClient client;

    /**
     * @Description  创建索引库
     * @Date  2019/5/30 16:09
     **/
    @Test
    public void testCreateIndex() throws IOException {
        //创建索引请求对象，并设置索引名称
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("my_course");

        //设置索引参数
        createIndexRequest.settings(
                Settings.builder().put("number_of_shards",1)
                                  .put("number_of_replicas",0));

        //设置映射
        createIndexRequest.mapping("doc","{\n" +
                        "\"properties\": {\n" +
                            "\"name\": {\n" +
                            "\"type\": \"text\",\n" +
                            "\"analyzer\":\"ik_max_word\",\n" +
                            "\"search_analyzer\":\"ik_smart\"\n" +
                        "},\n" +
                            "\"description\": {\n" +
                            "\"type\": \"text\",\n" +
                            "\"analyzer\":\"ik_max_word\",\n" +
                            "\"search_analyzer\":\"ik_smart\"\n" +
                        "},\n" +
                            "\"studymodel\": {\n" +
                            "\"type\": \"keyword\"\n" +
                        "},\n" +
                            "\"price\": {\n" +
                            "\"type\": \"float\"\n" +
                        "},\n" +
                            "\"timestamp\": {\n" +
                            "\"type\": \"date\",\n" +
                            "\"format\": \"yyyy‐MM‐dd HH:mm:ss||yyyy‐MM‐dd||epoch_millis\"\n" +
                        "}\n" +
                    "}\n" +
                "}", XContentType.JSON);

        //创建索引操作客户端
        IndicesClient indices = client.indices();

        //创建响应对象
        CreateIndexResponse createIndexResponse = indices.create(createIndexRequest, RequestOptions.DEFAULT);

        //得到响应结果
        boolean acknowledged = createIndexResponse.isAcknowledged();
         System.out.println(acknowledged);
    }

    /**
     * @Description  删除索引
     * @Date  2019/5/30 16:26
     **/
    public void testDeleteIndex() throws IOException {
       //删除索引请求对象
        DeleteIndexRequest indexRequest = new DeleteIndexRequest("my_course");
        //删除索引
        AcknowledgedResponse delete = client.indices().delete(indexRequest);
        //删除索引响应结果
        boolean acknowledged = delete.isAcknowledged();
        System.out.println(acknowledged);
    }


    /**
     * @Description  添加文档
     * @Date  2019/5/30 16:30
     **/
    @Test
    public void testAddDoc() throws IOException {
        //准备JSON数据
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "spring cloud实战");
        jsonMap.put("description", "本课程主要从四个章节进行讲解： 1.微服务架构入门 2.spring cloud 基础入门 3.实战Spring Boot 4.注册中心eureka。");
        jsonMap.put("studymodel", "201001");
        SimpleDateFormat dateFormat =new SimpleDateFormat("yyyy‐MM‐dd HH:mm:ss");
        jsonMap.put("timestamp", dateFormat.format(new Date()));
        jsonMap.put("price", 5.6f);
        //索引请求对象
        IndexRequest indexRequest = new IndexRequest("my_course","doc");

        //指定索引文档内容
        indexRequest.source(jsonMap);
        //索引响应对象
        IndexResponse index = client.index(indexRequest,RequestOptions.DEFAULT);

        //获取响应结果
        DocWriteResponse.Result result = index.getResult();
         System.out.println(result);

    }

    /**
     * @Description  查询文档
     * @Date  2019/5/30 16:39
     **/
    @Test
    public void getDoc() throws IOException {
        //GET /{index}/{type}/{id}
        GetRequest getRequest = new GetRequest("my_course","doc","XUMICGsBxM8fz5F0rbOS");

        GetResponse documentFields = client.get(getRequest,RequestOptions.DEFAULT);
        Map<String, Object> source = documentFields.getSourceAsMap();
         System.out.println(source);

    }

    /**
     * @Description  更新文档
     * @Date  2019/5/30 17:00
     **/
    @Test
    public void updateDoc() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("my_course","doc","XEPjB2sBxM8fz5F0J7O3");
        Map<String, String> map = new HashMap<>();
        map.put("name", "spring cloud实战");
        updateRequest.doc(map);
        UpdateResponse update = client.update(updateRequest,RequestOptions.DEFAULT);
        RestStatus status = update.status();
        System.out.println(status);
    }

    /**
     * @Description  删除文档
     * @Date  2019/5/30 17:09
     **/
    @Test
    public void testDelDoc() throws IOException {

        DeleteRequest deleteRequest = new DeleteRequest("my_course","doc","XEPjB2sBxM8fz5F0J7O3");
        DeleteResponse delete = client.delete(deleteRequest,RequestOptions.DEFAULT);

        DocWriteResponse.Result result = delete.getResult();
         System.out.println(result);
    }

}
