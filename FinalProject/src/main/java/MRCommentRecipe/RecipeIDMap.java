package MRCommentRecipe;

import Bean.CommentBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RecipeIDMap extends Mapper<LongWritable, Text, LongWritable, CommentBean>{
    LongWritable recipeID = new LongWritable();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Get one line
        String line = value.toString();
        String[] parseLine = line.split(",");

        //Use a method to determine this line is valid or not
//        boolean result = isValid(parseLine[2], context);
//        if(!result) return;
        try{
            recipeID.set(Long.parseLong(parseLine[1]));
            //Encapsulate the Bean
            CommentBean comment = new CommentBean();
            comment.setUser_id(Long.parseLong(parseLine[0]));
            comment.setRecipe_id(Long.parseLong(parseLine[1]));
            comment.setDate(parseLine[2]);
            comment.setRating(Integer.parseInt(parseLine[3]));

            StringBuilder sb = new StringBuilder();
            for(int i = 4; i < parseLine.length; i++) sb.append(parseLine[i]);
            comment.setReview(sb.toString());

            context.write(recipeID, comment);
        }catch (Exception e){
            return;
        }


    }

}
