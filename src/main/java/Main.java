import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Main class
 *
 * @author Administrator
 * @date 2018/9/27
 */
public class Main {
    private static final String LOG_INDEX  = "vsa_index_log";
    private static final String ASSET_INDEX  = "vsa_index_asset";
    private static final String TYPE  = "doc";
    public static void  main(String[] args){
        ESClientUtil.initEs();
        String[] fieldName={"event_diff"};
        String[] fieldValue={"1"};
        String[] aggFileds={"event_type"};
        Map<String,Object> map= ESClientUtil.multiFiledQueryAndAggs(fieldName,fieldValue,aggFileds,null,false);
//        ESClientUtil.commAgg(fieldName,fieldValue,null);
        int i=0;
/*        EventLog eventLog=new EventLog();
        eventLog.setDataType("LOG");
        eventLog.setDst_ip("192.168.1.102");
        eventLog.setSrc_ip("192.168.1.103");
        eventLog.setEvent_type("1");
        eventLog.setEvent_severity("1");
        eventLog.setEvent_name("异常行为");
        eventLog.setObject_ip("192.168.1.102");
        eventLog.setOccured_time("2018-09-05 14:05:11");
        eventLog.setEvent_status("0");
        eventLog.setSource_vendor("VAV");
        eventLog.setSource_ip("192.168.1.100");
        eventLog.setSource_platform("ETS");
        eventLog.setUpdate_time("2018-09-27 14:05:11");*/

/*        List<Map<Object, Object>> list = new ArrayList<Map<Object, Object>>();
        //资产库数据
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put("id",UUID.randomUUID().toString());
        map.put("datatype","ASSET");
        map.put("type","网络设备");
        map.put("nick_name","KX-03");
        map.put("ip","61.233.56.67" );
        map.put("mac","00-00-00-00-00-00");
        map.put("number","360011");
        map.put("organization","南昌市公安局");
        map.put("department","监所管理支队");
        map.put("area","");
        map.put("owner","李某某");
        map.put("phone","18666666666");
        map.put("classification","监所管理");
        map.put("computer_type","网络录像机");
        map.put("computer_purpose","0001");
        map.put("os","Windows7");
        map.put("up_ip","192.168.1.1");
        map.put("up_port","ethernet 0/0/1" );
        map.put("online_status","在线");
        map.put("access_status","1");
        map.put("describe","bei");
        map.put("last_online_time",new Date());
        map.put("life_status","0");
        map.put("source_vendor","VRV");
        map.put("source_platform", "ETS");
        map.put("source_ip", "192.168.1.2");
        map.put("ets_version", "1.0");
        map.put("os_install_time",new Date());
        map.put("last_communication_time",new Date());
        map.put("update_time",new Date());
        map.put("category","DEV");
        list.add(map);
        ESClientUtil.addDocuments(ASSET_INDEX,TYPE,list);*/

//事件库数据
//        List<Map<Object, Object>> list = new ArrayList<Map<Object, Object>>();
//        Map<Object, Object> map1 = new HashMap<Object, Object>();
//
//        map1.put("datatype", "log");
//        map1.put("dst_ip", "192.168.1.2");
//        map1.put("src_ip", "192.168.1.6");
//        map1.put("event_type", "1");
//        map1.put("event_severity","1");
//        map1.put("event_name", "警告行为");
//        map1.put("object_ip", "192.168.1.3");
//        map1.put("occured_time", new Date());
//        map1.put("event_status","2");
//        map1.put("source_vendor", "VAV");
//        map1.put("source_platform","ETS");
//        map1.put("source_ip", "192.168.1.5");
//        map1.put("update_time", new Date());
//        map1.put("handle_type", "1");
//        map1.put("relation_times",5);
//        map1.put("relation_ids","1,2");
//        map1.put("rule_id","2");
//        map1.put("event_diff", "0");


       // ESClientUtil.addDocuments(LOG_INDEX,TYPE,list);
//        List<Map<String,String>> list1= ESClientUtil.commAgg("event_type.keyword","0");
//
//        list.add(map1);
//      ESClientUtil.multiIndexQuery("192.168.",0,10);
     // ESClientUtil.createDocument(LOG_INDEX,TYPE,map1);
/*        Date currentTime = new Date(0);
        Date currentTime1 = new Date(Long.MAX_VALUE);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(formatter.format(currentTime));
        System.out.println(formatter.format(currentTime1));*/

//        EsHighLevelClientUtil.createIndex("my");

//        ESClientUtil.getEventLogNumByTime("2018-10-17 07:54:50","2018-10-17 08:54:50");

    }

}
