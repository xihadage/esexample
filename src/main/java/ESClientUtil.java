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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class ESClientUtil {
    private static TransportClient transportClient = null;
    private static final String LOG_INDEX = "vsa_index_log";
    private static final String ASSET_INDEX = "vsa_index_asset";
    private static final String TYPE = "doc";

    public static void initEs() {
        try {
            transportClient = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.3.61"), Integer.valueOf(9300)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static TransportClient getTransportClient() {
        return transportClient;
    }

    public static long getEventLogNumByTime(String startTime, String stopTime) {
        long eventNums = 0;
        try {
            String fromTime = TimeUtil.localtoUtc(startTime, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd HH:mm:ss");
            String endTime = TimeUtil.localtoUtc(stopTime, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd HH:mm:ss");
            QueryBuilder queryBuilder = QueryBuilders
                    .rangeQuery("occured_time")
                    .from(fromTime)
                    .to(endTime)
                    .includeLower(true)
                    .includeUpper(true);
            SearchResponse searchResponse = transportClient.prepareSearch(LOG_INDEX)
                    .setTypes(TYPE)
                    .setQuery(queryBuilder)
                    .execute()
                    .actionGet();
            eventNums = searchResponse.getHits().totalHits;
            return eventNums;
        } catch (Exception e) {
            // TODO: handle exception
            System.out.println("count error : " + e);
            return -1;
        }
    }

    static TermsAggregationBuilder getSubAggregationBuilder(int n, String[] aggField) {
        if(n<aggField.length) {
            String aggsName = "agg" + String.valueOf(n);
            TermsAggregationBuilder curtermsAggregationBuilder = AggregationBuilders.terms(aggsName).field(aggField[n]).size(Integer.MAX_VALUE);
            if (n==aggField.length-1){
                return curtermsAggregationBuilder;
            }
            TermsAggregationBuilder retTermsAggregationBuilder=curtermsAggregationBuilder.subAggregation(getSubAggregationBuilder(n + 1, aggField));
            return retTermsAggregationBuilder;
        }
        return null;
    }

    /**
     * 多字段嵌套分组查询
     * @param fieldName
     * @param fieldValue
     * @param aggField
     * @return
     */
    public static Map<String, Object> commAgg(String[] fieldName, String[] fieldValue, String[] aggField) {
        Map<String, Object> allMap = new HashMap<>();
        try {
            //过滤
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            if (fieldName != null && fieldValue != null) {
                for (int i = 0; i < fieldName.length; i++) {
                    boolQueryBuilder.must(QueryBuilders.termQuery(fieldName[i], fieldValue[i]));
                }
            }

            //聚合
            AggregationBuilder termsAggregationBuilder = null;
            if (aggField != null){
                termsAggregationBuilder=getSubAggregationBuilder(0,aggField);
            }

            //执行
            SearchResponse searchResponse;
            SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(LOG_INDEX).setTypes(TYPE);
            if(termsAggregationBuilder==null){
                searchResponse = searchRequestBuilder.setQuery(boolQueryBuilder).execute().actionGet();
            }else{
                searchResponse = searchRequestBuilder.setQuery(boolQueryBuilder).addAggregation(termsAggregationBuilder).execute().actionGet();
            }
            //取查询结果
            List<String> jsons = new ArrayList<>();
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                String json = hit.getSourceAsString();
                jsons.add(json);
            }
            allMap.put("queryRet", jsons);
            //取聚合结果

        } catch (Exception e) {
            System.out.println("commonAgg error : " + e);
        }
        return allMap;
    }


    /**
     * 多字段平铺嵌套查询
     *
     * @param fieldName
     * @param fieldValue
     * @param aggField
     * @return
     */
    public static Map<String, Object> multiFiledQueryAndAggs(String[] fieldName, String[] fieldValue, String[] aggField) {

        Map<String, Object> allMap = new HashMap<>();
        try {
            //过滤
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            if (fieldName != null && fieldValue != null) {
                for (int i = 0; i < fieldName.length; i++) {
                    boolQueryBuilder.must(QueryBuilders.termQuery(fieldName[i], fieldValue[i]));
                }
            }

            //聚合
            AggregationBuilder termsAggregationBuilder0 = null;
            if (aggField != null) {
                for (int i = 0; i < aggField.length; i++) {
                    if (i == 0) {
                        termsAggregationBuilder0 = AggregationBuilders.terms("agg0").field(aggField[i]).size(Integer.MAX_VALUE);
                    } else {
                        String aggsName = "agg" + String.valueOf(i);
                        TermsAggregationBuilder curtermsAggregationBuilder = AggregationBuilders.terms(aggsName).field(aggField[i]).size(Integer.MAX_VALUE);
                        termsAggregationBuilder0.subAggregation(curtermsAggregationBuilder);
                    }
                }
            }

            //执行
            SearchResponse searchResponse;
            SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(LOG_INDEX).setTypes(TYPE);
            if (termsAggregationBuilder0 == null) {
                searchResponse = searchRequestBuilder.setQuery(boolQueryBuilder).execute().actionGet();
            } else {
                searchResponse = searchRequestBuilder.setQuery(boolQueryBuilder).addAggregation(termsAggregationBuilder0).execute().actionGet();
            }


            //取查询结果
            List<String> jsons = new ArrayList<>();
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                String json = hit.getSourceAsString();
                jsons.add(json);
            }
            allMap.put("queryRet", jsons);

            //取聚合结果
            if (termsAggregationBuilder0 != null) {
                List<Map<String, Object>> retList = new ArrayList<>();
                Terms agg = searchResponse.getAggregations().get("agg0");
                for (Terms.Bucket entry : agg.getBuckets()) {
                    String key = entry.getKeyAsString();
                    Map<String, Object> subMap1 = new HashMap<>();
                    for (int i = 1; i < aggField.length; i++) {
                        Map<String, Object> subMap2 = new HashMap<>();
                        String aggsName = "agg" + String.valueOf(i);
                        Terms rule = entry.getAggregations().get(aggsName);
                        for (Terms.Bucket bucket : rule.getBuckets()) {
                            subMap2.put(bucket.getKeyAsString(), bucket.getDocCount());
                        }
                        subMap1.put(aggField[i], subMap2);
                    }
                    Map<String, Object> retMap = new HashMap<>();
                    retMap.put("name", key);
                    retMap.put("count", entry.getDocCount());
                    retMap.put("value", subMap1);
                    retList.add(retMap);
                }
                allMap.put("AggsRet", retList);
            }
        } catch (Exception e) {
            System.out.println("commonAgg error : " + e);
        }
        return allMap;
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
    public static void createDocument(String index, String type, Object object) {
        try {
//            ObjectMapper objectMapper = new ObjectMapper();
//            String json = objectMapper.writeValueAsString(object);
            String json = JSON.toJSONString(object);
            transportClient.prepareIndex(index, type, UUID.randomUUID().toString()).setSource(json).get();
            System.out.println("添加数据成功");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("添加数据失败");
        }
    }

    //批量创建文档 XContentBuilder
    public static void addDocuments(String index, String type, List<Map<Object, Object>> list) {
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
    public static void testQuery(String index, String type, String text, int start, int length) {
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
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("查询失败");
        }
    }

    //模糊查询
    public static void multiIndexQuery(String text, int start, int length) {
        try {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            baseCondition(boolQueryBuilder, text);

            HighlightBuilder highlightBuilder = new HighlightBuilder();
            highlightBuilder.preTags();
            highlightBuilder.postTags();
            highlightBuilder.field("*");

            SearchRequestBuilder searchRequestBuilder = transportClient
                    .prepareSearch(LOG_INDEX, ASSET_INDEX)
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
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("查询失败");
        }
    }

    //查询条件
    public static void baseCondition(BoolQueryBuilder boolQueryBuilder, String text) {
        boolQueryBuilder.must(QueryBuilders.boolQuery()
                .must(QueryBuilders.multiMatchQuery(text).lenient(true)));
    }

    public static void logConditon(BoolQueryBuilder boolQueryBuilder, String text) {
        boolQueryBuilder.must(QueryBuilders.boolQuery()
                .must(QueryBuilders.multiMatchQuery(text).lenient(true))
                .should(QueryBuilders.multiMatchQuery(text, "*_name")
                        .boost(5)));
    }

    public static void assertConditon(BoolQueryBuilder boolQueryBuilder, String text) {
        boolQueryBuilder.must(QueryBuilders.boolQuery()
                .must(QueryBuilders.multiMatchQuery(text).lenient(true))
                .should(QueryBuilders.multiMatchQuery(text, "ip", "*_ip", "owner")
                        .boost(5)));
    }
}
