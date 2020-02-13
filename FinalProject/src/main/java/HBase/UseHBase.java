package HBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class UseHBase {
    public Configuration configuration;
    public Connection connection;
    public Admin admin;

    public void init(){
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbasedata");
        configuration.set("hbase.zookeeper.quorum","localhost:2181");
        //54442 org.apache.zookeeper.server.quorum.QuorumPeerMain
        //"hbase.zookeeper.property.clientPort", "2181"
        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            if (admin != null) admin.close();
            if (null != connection) connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createTable(String myTableName,String[] colFamily) throws IOException {
        //init();
        TableName tableName = TableName.valueOf(myTableName);
        try{
            if(admin.tableExists(tableName)){
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }else {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                for(String str : colFamily){
                    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
                    hTableDescriptor.addFamily(hColumnDescriptor);
                }
                admin.createTable(hTableDescriptor);
            }
            close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void insertRow(String tableName, String rowKey, String colFamily, String col, String val) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
        close();
    }

    public Result getData(String tableName,String rowKey,String colFamily,String col)throws IOException{
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(colFamily.getBytes(),col.getBytes());
        Result result = table.get(get);
        table.close();
        close();
        return result;
    }


}
