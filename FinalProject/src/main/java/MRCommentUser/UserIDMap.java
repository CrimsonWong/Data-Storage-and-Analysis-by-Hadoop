package MRCommentUser;

import Bean.CommentBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserIDMap extends Mapper<LongWritable, Text, LongWritable, CommentBean>{
    LongWritable userID = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Get one line
        String line = value.toString();
        String[] parseLine = line.split(",");

        //Use a method to determine this line is valid or not
        //boolean result = isValid(parseLine[2], context);
        //if(!result) return;

        try{
            userID.set(Long.parseLong(parseLine[0]));
            //Encapsulate the Bean
            CommentBean comment = new CommentBean();
            comment.setUser_id(Long.parseLong(parseLine[0]));
            comment.setRecipe_id(Long.parseLong(parseLine[1]));
            comment.setDate(parseLine[2]);
            comment.setRating(Integer.parseInt(parseLine[3]));

            StringBuilder sb = new StringBuilder();
            for(int i = 4; i < parseLine.length; i++) sb.append(parseLine[i]);
            comment.setReview(sb.toString());

            context.write(userID, comment);
        } catch (NumberFormatException e){
            return;
        }

    }

    private boolean isValid(String date, Context context){
        //String pattern = "[A-Za-z0-9\\s\\.\\-]+";
        if(!date.matches("\\d{4}-\\d{2}-\\d{2}")) return false;
        return true;
    }
}
