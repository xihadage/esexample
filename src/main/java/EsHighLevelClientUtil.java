import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * EsHighLevelClientUtil class
 *
 * @author Administrator
 * @date 2018/10/12
 */
public class EsHighLevelClientUtil {

    private static ObjectMapper mapper = new ObjectMapper();
    private static RestHighLevelClient client = RestClientFactory.getHighLevelClient();
    /**
     * 创建索引
     *
     * @param index
     * @return
     */
    public static boolean createIndex(String index) {
        //index名必须全小写，否则报错
        CreateIndexRequest request = new CreateIndexRequest(index);
        try {
            CreateIndexResponse indexResponse = client.indices().create(request);
            if (indexResponse.isAcknowledged()) {
                System.out.println("创建索引成功");
            } else {
                System.out.println("创建索引失败");
            }
            return indexResponse.isAcknowledged();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 插入数据
     *
     * @param index
     * @param type
     * @param object
     * @return
     */
    public static String addData(String index, String type, String id, JSONObject object) {
        IndexRequest indexRequest = new IndexRequest(index, type, id);
        try {
            indexRequest.source(mapper.writeValueAsString(object), XContentType.JSON);
            IndexResponse indexResponse = client.index(indexRequest);
            return indexResponse.getId();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 检查索引
     *
     * @param index
     * @return
     * @throws IOException
     */
    public static boolean checkIndexExist(String index) {
        try {
            Response response = client.getLowLevelClient().performRequest("HEAD", index);
            boolean exist = response.getStatusLine().getReasonPhrase().equals("OK");
            return exist;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
    /**
     * 查询所有
     *
     * @return
     */
    public static String queryAll(String indexName, String esType) {
        try {
            HttpEntity entity = new NStringEntity(
                    "{ \"query\": { \"match_all\": {}}}",
                    ContentType.APPLICATION_JSON);
            String endPoint = "/" + indexName + "/" + esType + "/_search";
            Response response = client.getLowLevelClient().performRequest("POST", endPoint, Collections.<String, String>emptyMap(), entity);
            return EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "查询数据出错";
    }

    /**
     * 根据条件查询
     *
     * @return
     */
    public static String queryByMatch(String indexName, String esType, String fieldKey, String fieldVal) {
        try {
            String endPoint = "/" + indexName + "/" + esType + "/_search";

            IndexRequest indexRequest = new IndexRequest();
            XContentBuilder builder;
            try {
                builder = JsonXContent.contentBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("match")
                        .field(fieldKey, fieldVal)
                        .endObject()
                        .endObject()
                        .endObject();
                indexRequest.source(builder);
            } catch (IOException e) {
                e.printStackTrace();
            }

            String source = indexRequest.source().utf8ToString();

            System.out.println("source---->" + source);

            HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);

            Response response = client.getLowLevelClient().performRequest("POST", endPoint, Collections.<String, String>emptyMap(), entity);
            return EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "查询数据出错";
    }


    /**
     * 复合查询
     *
     * @return
     */
    public static String queryByCompound(String indexName, String esType) {
        try {
            String endPoint = "/" + indexName + "/" + esType + "/_search";

            IndexRequest indexRequest = new IndexRequest();
            XContentBuilder builder;
            try {
                /**
                 * 查询名字等于 liming
                 * 并且年龄在30和35之间
                 */
                builder = JsonXContent.contentBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("bool")
                        .startObject("must")
                        .startObject("match")
                        .field("name.keyword", "liming")
                        .endObject()
                        .endObject()
                        .startObject("filter")
                        .startObject("range")
                        .startObject("age")
                        .field("gte", "30")
                        .field("lte", "35")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject();
                indexRequest.source(builder);
            } catch (IOException e) {
                e.printStackTrace();
            }

            String source = indexRequest.source().utf8ToString();

            System.out.println("source---->" + source);

            HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);

            Response response = client.getLowLevelClient().performRequest("POST", endPoint, Collections.<String, String>emptyMap(), entity);
            return EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "查询数据出错";
    }

    /**
     * 演示聚合统计
     *
     * @return
     */
    public static String aggregation(String indexName, String esType, String deleteField, String deleteText) {
        try {
            String endPoint = "/" + indexName + "/" + esType + "/_search";

            IndexRequest indexRequest = new IndexRequest();
            XContentBuilder builder;
            try {
                builder = JsonXContent.contentBuilder()
                        .startObject()
                        .startObject("aggs")
                        .startObject("名称分组结果")
                        .startObject("terms")
                        .field("field", "name.keyword")
                        .startArray("order")
                        .startObject()
                        .field("_count", "asc")
                        .endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject();
                indexRequest.source(builder);
            } catch (IOException e) {
                e.printStackTrace();
            }

            String source = indexRequest.source().utf8ToString();

            System.out.println("source---->" + source);

            HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);

            Response response = client.getLowLevelClient().performRequest("POST", endPoint, Collections.<String, String>emptyMap(), entity);
            return EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "查询数据出错";
    }

    /**
     *  同步获取操作结果
     */
    public static IndexResponse postRequest(String index, String type, String id, String jsonSource)
            throws Exception {
        // 构建请求
        IndexRequest request = new IndexRequest(index, type, id);
        // 将保存数据以JSON格式关联到请求
        request.source(jsonSource, XContentType.JSON);
        // Java客户端发起保存数据请求
        IndexResponse response = RestClientFactory.getHighLevelClient().index(request);
        // 等待结果
        System.out.println(response);
        return response;
    }
    /**
     * @param keyword1 关键字1
     * @param keyword2 关键字2
     * @param startDate 起始时间
     * @param endDate 终止时间
     *
     **/
    public  static SearchResponse pageQueryRequest(String keyword1, String keyword2, String startDate, String endDate,
                                                   int start, int size){
        RestHighLevelClient client = RestClientFactory.getHighLevelClient();

        // 这个sourcebuilder就类似于查询语句中最外层的部分。包括查询分页的起始，
        // 查询语句的核心，查询结果的排序，查询结果截取部分返回等一系列配置
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 结果开始处
        sourceBuilder.from(start);
        // 查询结果终止处
        sourceBuilder.size(size);
        // 查询的等待时间
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        MatchQueryBuilder matchbuilder;
        matchbuilder = QueryBuilders.matchQuery("message", keyword1+" "+keyword2);
        // 同时满足两个关键字
        matchbuilder.operator(Operator.AND);
        // 查询在时间区间范围内的结果
        RangeQueryBuilder rangbuilder = QueryBuilders.rangeQuery("date");
        if(!"".equals(startDate)){
            rangbuilder.gte(startDate);
        }
        if(!"".equals(endDate)){
            rangbuilder.lte(endDate);
        }
        // 等同于bool，将两个查询合并
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        boolBuilder.must(matchbuilder);
        boolBuilder.must(rangbuilder);
        // 排序
        FieldSortBuilder fsb = SortBuilders.fieldSort("date");
        fsb.order(SortOrder.DESC);
        sourceBuilder.sort(fsb);

        sourceBuilder.query(boolBuilder);
        SearchRequest searchRequest = new SearchRequest("request");
        searchRequest.types("doc");
        searchRequest.source(sourceBuilder);
        SearchResponse response = null;
        try {
            response = client.search(searchRequest);
            SearchHits hits= response.getHits();
            int totalRecordNum= (int) hits.getTotalHits();
/*          JSONObject json = new JSONObject();
            json.put("date","1995-05-16");
            for (SearchHit searchHit : hits) {
                Map<String, Object> source = searchHit.getSourceAsMap();
                User user = JSON.parseObject(json.toString(),User.class);
                System.out.println(user);
            }*/
            client.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return response;
    }
}
