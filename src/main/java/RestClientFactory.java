import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * EsHighLevelClient class
 *
 * @author Administrator
 * @date 2018/10/12
 */
public class RestClientFactory {
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 9200;
    private static final String SCHEMA = "http";
    private static final int CONNECT_TIME_OUT = 1000;
    private static final int SOCKET_TIME_OUT = 30000;
    private static final int CONNECTION_REQUEST_TIME_OUT = 500;

    private static final int MAX_CONNECT_NUM = 100;
    private static final int MAX_CONNECT_PER_ROUTE = 100;

    private static HttpHost HTTP_HOST = new HttpHost(HOST,PORT,SCHEMA);
    private static boolean uniqueConnectTimeConfig = false;
    private static boolean uniqueConnectNumConfig = true;
    private static RestClientBuilder builder;
    private static RestClient restClient;
    private static RestHighLevelClient restHighLevelClient;

    static {
        init();
    }
    public static void init(){
        builder = RestClient.builder(HTTP_HOST);
        if(uniqueConnectTimeConfig){
            setConnectTimeOutConfig();
        }
        if(uniqueConnectNumConfig){
            setMutiConnectConfig();
        }
        restClient = builder.build();
        restHighLevelClient = new RestHighLevelClient(builder);
    }
    /**
     *   主要关于异步httpclient的连接延时配置
     */
    public static void setConnectTimeOutConfig(){
        builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder builder) {
                builder.setConnectTimeout(CONNECT_TIME_OUT);
                builder.setSocketTimeout(SOCKET_TIME_OUT);
                builder.setConnectionRequestTimeout(CONNECTION_REQUEST_TIME_OUT);
                return builder;
            }
        });
    }
    /**
     *    主要关于异步httpclient的连接数配置
     */
    public static void setMutiConnectConfig(){
        builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                httpAsyncClientBuilder.setMaxConnTotal(MAX_CONNECT_NUM);
                httpAsyncClientBuilder.setMaxConnPerRoute(MAX_CONNECT_PER_ROUTE);
                return httpAsyncClientBuilder;
            }
        });
    }
    public static RestClient getClient(){
        return restClient;
    }

    public static RestHighLevelClient getHighLevelClient(){
        if(restHighLevelClient==null){
            init();
        }
        return restHighLevelClient;
    }

    public static void close() {
        if (restHighLevelClient != null) {
            try {
                restHighLevelClient.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
