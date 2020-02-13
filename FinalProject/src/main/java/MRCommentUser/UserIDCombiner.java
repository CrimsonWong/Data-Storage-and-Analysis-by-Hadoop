package MRCommentUser;

import Bean.CommentBean;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class UserIDCombiner extends Reducer<LongWritable, CommentBean, Text, IntWritable>{

    @Override
    protected void reduce (LongWritable key, Iterable<CommentBean> values, Context context) throws IOException, InterruptedException{
        int totalRating = 0;
        int count = 0;
        for(CommentBean val : values){
            count += 1;
            totalRating += val.getRating();
        }

        context.write(new Text(Long.toString(key.get())), new IntWritable(count));
    }


}
