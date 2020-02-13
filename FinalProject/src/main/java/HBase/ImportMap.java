package HBase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ImportMap extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split(",");
        try{
            outKey.set(splits[0]);
            outValue.set(splits[1]+","+splits[2]+","+splits[3]+","+splits[4]);
            context.write(outKey, outValue);
        } catch (NumberFormatException e){
            return;
        }
    }
}
