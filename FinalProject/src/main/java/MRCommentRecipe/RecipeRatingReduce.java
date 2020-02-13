package MRCommentRecipe;

import Bean.CommentBean;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RecipeRatingReduce extends Reducer<LongWritable, CommentBean, Text, IntWritable>{
    int count = 0;
    int rate = 0;
    @Override
    protected void reduce (LongWritable key, Iterable<CommentBean> values, Context context) throws IOException, InterruptedException{
        for(CommentBean val : values){
            count += 1;
            rate = val.getRating();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String result = "Number of Recipes of Rating" + rate + ": ";
        context.write(new Text(result), new IntWritable(count));
    }

}
