package MRCommentUser;

import Bean.CommentBean;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class UserIDReduceCount extends Reducer<Text, IntWritable, Text, IntWritable>{
    int sum = 0;

    @Override
    protected void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

        for(IntWritable val : values){
            sum += val.get();
        }

        context.write(new Text("Total Comments: "), new IntWritable(sum));
    }


}
