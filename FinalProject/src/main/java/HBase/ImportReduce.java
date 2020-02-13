package HBase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Map;

public class ImportReduce extends TableReducer<Text, Text, NullWritable> {
    UseHBase use;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        use = new UseHBase();
//        String[] columnFamily = new String[]{"name","comment"};
//        use.createTable("finalProject", columnFamily);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Put put = new Put(Bytes.toBytes(key.toString()));
        for(Text value : values){
            String[] parseLine = value.toString().split(",");
            put.addColumn("name".getBytes(), "name".getBytes(), parseLine[0].getBytes());
            put.addColumn("comment".getBytes(), "userId".getBytes(), parseLine[1].getBytes());
            put.addColumn("comment".getBytes(), "date".getBytes(), parseLine[2].getBytes());
            put.addColumn("comment".getBytes(), "rating".getBytes(), parseLine[3].getBytes());

//            put.addColumn(Bytes.toBytes("comment"), Bytes.toBytes("userId"), Bytes.toBytes(parseLine[1]));
//            put.addColumn(Bytes.toBytes("comment"), Bytes.toBytes("date"), Bytes.toBytes(parseLine[2]));
//            put.addColumn(Bytes.toBytes("comment"), Bytes.toBytes("rating"), Bytes.toBytes(parseLine[3]));
        }
        context.write(NullWritable.get(),put);
    }
}
