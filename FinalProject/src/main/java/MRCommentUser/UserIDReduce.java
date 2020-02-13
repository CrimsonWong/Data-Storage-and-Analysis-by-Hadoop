package MRCommentUser;

import Bean.CommentBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class UserIDReduce extends Reducer<LongWritable, CommentBean, Text, Text>{

    private TreeMap<Integer, Long> activeUser;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        activeUser = new TreeMap<Integer, Long>();
    }

    @Override
    protected void reduce (LongWritable key, Iterable<CommentBean> values, Context context) throws IOException, InterruptedException{
        int totalRating = 0;
        int count = 0;
        for(CommentBean val : values){
            count += 1;
            totalRating += val.getRating();
            //context.write(new Text(Long.toString(key.get())), new Text(Integer.toString(val.getRating())));
        }
        //Put into the activeUser Map and find out the top 10
        activeUser.put(count, key.get());
        if(activeUser.size() > 10) activeUser.remove(activeUser.firstKey());

        float avg = totalRating/count;
        String result = count + ", " + avg;
        context.write(new Text(Long.toString(key.get())), new Text(result));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String key = "The 10 most active user: ";
        StringBuilder value = new StringBuilder();
        for (Map.Entry<Integer, Long> entry : activeUser.entrySet()) value.append(entry.getValue().toString()+" ");
        context.write(new Text(key),new Text(value.toString()));
    }
}
