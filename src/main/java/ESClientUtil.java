/**
 * ESClientUtil class
 *
 * @author Administrator
 * @date 2018/9/27
 */

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ESClientUtil {
    private static TransportClient transportClient = null;
    private static final String LOG_INDEX  = "vsa_index_log";
    private static final String ASSET_INDEX  = "vsa_index_asset";
    private static final String TYPE  = "doc";
    public static void initEs(){
        try {
            transportClient = new PreBuiltTransportClient(Settings.EMPTY)
                              .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.3.61"), Integer.valueOf(9300)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static TransportClient getTransportClient(){
        return transportClient;
    }

    //创建索引
    public static void createIndex(String index) {
        //index名必须全小写，否则报错
        try {
            transportClient.admin().indices().prepareCreate(index).get();
            System.out.println("创建索引成功");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("创建索引失败");
        }
    }
   //删除索引
    public static void deleteIndex(String index) {
        //index名必须全小写，否则报错
        try {
            transportClient.admin().indices().prepareDelete(index).get();
            System.out.println("删除索引成功");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("删除索引失败");
        }
    }
   //创建文档java object?
    public static void createDocument(String index,String  type,Object object){
        try {
//            ObjectMapper objectMapper = new ObjectMapper();
//            String json = objectMapper.writeValueAsString(object);
            String json= JSON.toJSONString(object);
            transportClient.prepareIndex(index, type, UUID.randomUUID().toString()).setSource(json).get();
            System.out.println("添加数据成功");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("添加数据失败");
        }
    }
   //批量创建文档 XContentBuilder
    public static void addDocuments( String index,String type,List<Map<Object, Object>> list) {
        try {

            BulkRequestBuilder bulkRequest = transportClient.prepareBulk();
            for (Map<Object, Object> map : list) {
                //遍历map所有field,构造插入对象
                XContentBuilder xb = XContentFactory.jsonBuilder().startObject();
                for (Object key : map.keySet()) {
                    xb.field((String) key, map.get(key));
                }
                xb.endObject();
                bulkRequest.add(transportClient.prepareIndex(index, type, UUID.randomUUID().toString()).setSource(xb));
            }
            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                System.err.println("失败");
            }
            System.out.println("添加数据成功");
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.out.println("添加数据失败");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.out.println("添加数据失败");
        }
    }

    //模糊查询
    public static void testQuery(String index,String type, String text,int start,int length){
        try {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            baseCondition(boolQueryBuilder, text);
            SearchRequestBuilder searchRequestBuilder = transportClient
                    .prepareSearch(index)
                    .setTypes(type)
                    .setQuery(boolQueryBuilder)
                    .setFrom(start)
                    .setSize(length);
            SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHists = hits.getHits();
            System.out.println(searchHists.length);
            System.out.println("查询成功");
        }catch (Exception e) {
            e.printStackTrace();
            System.out.println("查询失败");
        }
    }
    //模糊查询
    public static void multiIndexQuery(String text,int start,int length){
        try {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            baseCondition(boolQueryBuilder, text);

            HighlightBuilder highlightBuilder=new HighlightBuilder();
            highlightBuilder.preTags();
            highlightBuilder.postTags();
            highlightBuilder.field("*");

            SearchRequestBuilder searchRequestBuilder = transportClient
                    .prepareSearch(LOG_INDEX,ASSET_INDEX)
                    .setTypes(TYPE)
                    .setQuery(boolQueryBuilder)
                    .highlighter(highlightBuilder)
                    .setFrom(start)
                    .setSize(length);
            SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHists = hits.getHits();
            System.out.println(searchHists.length);
            System.out.println("查询成功");
        }catch (Exception e) {
            e.printStackTrace();
            System.out.println("查询失败");
        }
    }

    //查询条件
    public  static  void baseCondition(BoolQueryBuilder boolQueryBuilder,String text){
        boolQueryBuilder.must(QueryBuilders.boolQuery()
                .must(QueryBuilders.multiMatchQuery(text).lenient(true)));
    }

    public static void logConditon(BoolQueryBuilder boolQueryBuilder,String text){
        boolQueryBuilder.must(QueryBuilders.boolQuery()
                .must(QueryBuilders.multiMatchQuery(text).lenient(true))
                .should(QueryBuilders.multiMatchQuery(text, "*_name")
                        .boost(5)));
    }
    public static void assertConditon(BoolQueryBuilder boolQueryBuilder,String text){
        boolQueryBuilder.must(QueryBuilders.boolQuery()
                .must(QueryBuilders.multiMatchQuery(text).lenient(true))
                .should(QueryBuilders.multiMatchQuery(text, "ip","*_ip","owner")
                        .boost(5)));
    }
}
