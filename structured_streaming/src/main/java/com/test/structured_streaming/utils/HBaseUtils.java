package com.test.structured_streaming.utils;
import com.alibaba.fastjson.JSONObject;
import io.leopard.javahost.JavaHost;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.util.*;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/3/31 13:33
 */
public class HBaseUtils {


    //1.静态代码块获取连接对象
    static Connection connection = null;

    static {
        Resource resource = new ClassPathResource("/vdns.properties");
        Properties props = null;
        try {
            props = PropertiesLoaderUtils.loadProperties(resource);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("props"+props);
        JavaHost.updateVirtualDns(props);
        JavaHost.printAllVirtualDns();// 打印所有虚拟DNS记录
        //设置zookeeper
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "172_21_0_8");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        //获取连接对象
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //2.获取表
    public static Table getTable(String tableName) {

        Table tblName = null;
        try {
            tblName = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tblName;
    }

    //3.插入单列数据
    public static void putDataByRowkey(String tableName, String family, String colName, String colValue, String rowkey) {
        //获取表
        Table table = getTable(tableName);

        try {
            //新建put对象
            Put put = new Put(rowkey.getBytes());
            //封装数据
            put.addColumn(family.getBytes(), Bytes.toBytes(colName), colValue.getBytes());

            //数据插入
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                //表关闭
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //4.插入多列数据
    public static void putMapDataByRowkey(String tableName, String family, Map<String, Object> map, String rowkey) {

        Table table = getTable(tableName);
        try {
            Put put = new Put(rowkey.getBytes());
            //map的key是列名，value是列值
            for (String key : map.keySet()) {

                put.addColumn(family.getBytes(), key.getBytes(), map.get(key).toString().getBytes());
            }
            //插入
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //5.根据rowkey查询数据
    public static String queryByRowkey(String tableName, String family, String colName, String rowkey) {

        Table table = getTable(tableName);
        String str = null;
        try {
            //查询
            Get get = new Get(rowkey.getBytes());
            Result result = table.get(get);
            byte[] value = result.getValue(family.getBytes(), colName.getBytes());
            str = new String(value);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return str;
    }

    //6.根据rowkey删除数据
    public static void delByRowkey(String tableName, String family, String rowkey) {

        Table table = getTable(tableName);
        try {
            //删除对象
            Delete delete = new Delete(rowkey.getBytes());
            //添加列簇
            delete.addFamily(family.getBytes());
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //7.批量数据插入
    public static void putList(String tableName, List<Put> list) {

        Table table = getTable(tableName);
        try {
            table.put(list);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public Map<String,String> getOneRow(String tableName, String rowKey, Map<String,String> colls){
        Table table = null;
        Map<String,String> returnData = new HashMap<>();
        try {
            table = getTable(tableName);
            System.out.println("bbbbbbbbbbbbbbbb");
            Get get = new Get(rowKey.getBytes());
            if(colls != null){
                for(Map.Entry<String, String> entry : colls.entrySet()){
                    System.out.println("111111111111");
                    get.addColumn(entry.getKey().getBytes(), entry.getValue().getBytes());
                }
            }
            Result result = table.get(get);
            System.out.println("222222222222222222");
            cellsToMap(result.listCells(), returnData);
            return returnData;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static String queryOneByRowkey(String tableName, String family, String colName, String rowkey) {
        System.out.println("tableName"+tableName+"="+family+"--"+colName+"--"+rowkey);
        Table table = getTable(tableName);
        System.out.println(table);
        String str = null;
        try {
            //查询
            System.out.println("11111111");
            Get get = new Get(rowkey.getBytes());
            System.out.println("get"+get);
            System.out.println("2222222");
            Result result = table.get(get);
            System.out.println("33333333");
            byte[] value = result.getValue(family.getBytes(), colName.getBytes());
            System.out.println("44444444");
            str = new String(value);
            System.out.println("555555555");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return str;
    }

    /**
     *将cell转换为map
     */
    private Map<String,String> cellsToMap(List<Cell> cells, Map<String,String> map){
        for(Cell cell : cells){
            map.put(
                    String.format(
                            "%s:%s",
                            Bytes.toString(CellUtil.cloneFamily(cell)),
                            Bytes.toString(CellUtil.cloneQualifier(cell))),
                    Bytes.toString(CellUtil.cloneValue(cell)));
        }
        return map;
    }



    public static List<Map<String,Object>> getAllData(String tableName){

        Table table = null;
        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        try {
            table = getTable(tableName);
            ResultScanner results = null;
            try {
                results = table.getScanner(new Scan());
            } catch (IOException e) {
                e.printStackTrace();
            }

            for (Result result : results) {
                Map<String, Object> map = new HashMap<>();
                 map.put("rowkey",new String(result.getRow()));
                for (Cell cell : result.rawCells()) {
//                    String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    //String family =  Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                    String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    map.put(colName,value);

                 }
                list.add(map);
            }
        }catch (Exception e){
        e.printStackTrace();
        }
        System.out.println(list);
        return  list;
    }



    public static Map<String, Object> getOneRow(String tableName, String row) throws Exception {
        String returnStr = "";
        Table table = getTable(tableName);
        Get get = new Get(Bytes.toBytes(row));
         Result result = table.get(get);
         Map <String ,Object> map=null;
         if(!result.isEmpty()) {
            map=new HashMap<>();
            map.put("rowkey", new String(result.getRow()));

            for (KeyValue rowKV : result.raw()) {
//            returnStr += "  RowName:" + new String(rowKV.getRow()) +" ,";
//            returnStr += "Timestamp:" + rowKV.getTimestamp() +" ,";
//            returnStr += "FamilyName:" + new String(rowKV.getFamily()) +" ,";
                map.put(new String(rowKV.getQualifier()), new String(rowKV.getValue()));

            }
        }
        return map;
    }

    public static void main(String[] args) throws IOException {

        getAllData("ns1:t2");
        try {
            Map<String, Object> oneRow = getOneRow("ns1:t2", "1");
            String s = JSONObject.toJSONString(oneRow);
            System.out.println("s"+s);
            Iterator iter = oneRow.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                Object key = entry.getKey();
                Object val = entry.getValue();
                System.out.println(key+"=="+val);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

//         Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "172_21_0_8");
//        conf.set("hbase.zookeeper.property.client", "2181");
//
//        Connection conn = ConnectionFactory.createConnection(conf);
//        Admin admin = conn.getAdmin();
//        System.out.println("=--------");
////        System.out.println(admin.tableExists(TableName.valueOf("ns1:t1")));
//        Table table = conn.getTable(TableName.valueOf("ns1:t2"));
//
//        Scan scan = new Scan();
//        scan.setStartRow("1".getBytes());
//        System.out.println(table.toString());
//        ResultScanner scanner = table.getScanner(scan);
//        System.out.println("=--------" + scanner.toString());
//        for (Result res : scanner) {
//            if (res.raw().length == 0) {
//                System.out.println("ns1:t1" + " 表数据为空！");
//            } else {
//                for (KeyValue kv : res.raw()) {
//                    System.out.println(new String(kv.getKey()) + "\t" + new String(kv.getValue()));
//                }
//            }
//        }





    }


}

