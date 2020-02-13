package MRCommentRecipe;

import Bean.CommentBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class RecipeIDReduce extends Reducer<LongWritable, CommentBean, Text, Text>{
    private TreeMap<Double, Long> topWelcomedRecipe;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        topWelcomedRecipe = new TreeMap<Double, Long>();
    }

    @Override
    protected void reduce (LongWritable key, Iterable<CommentBean> values, Context context) throws IOException, InterruptedException{
        double totalRating = 0;
        int count = 0;
        for(CommentBean val : values){
            count += 1;
            totalRating += val.getRating();
        }

        double avg = totalRating/count;
        //Put into the activeUser Map and find out the top 10
        topWelcomedRecipe.put(avg, key.get());
        if(topWelcomedRecipe.size() > 10) topWelcomedRecipe.remove(topWelcomedRecipe.firstKey());

        String result = count + ", " + avg;
        context.write(new Text(key.toString()), new Text(result));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String key = "The 10 most welcomed recipes: ";
        StringBuilder value = new StringBuilder();
        for (Map.Entry<Double, Long> entry : topWelcomedRecipe.entrySet()) value.append(entry.getValue().toString()+" ");
        context.write(new Text(key),new Text(value.toString()));
    }
}
