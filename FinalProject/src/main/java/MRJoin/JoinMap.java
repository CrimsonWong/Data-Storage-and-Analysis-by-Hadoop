package MRJoin;

import Bean.JoinBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JoinMap extends Mapper<LongWritable, Text, LongWritable, JoinBean> {
    String name;
    LongWritable outKey = new LongWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //Get input file name
        FileSplit input = (FileSplit)context.getInputSplit();
        name = input.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parseLine = line.split(",");
        JoinBean bean = new JoinBean();
        try{
            if(name.startsWith("comments")){
                bean.setRecipe_id(Long.parseLong(parseLine[1]));
                bean.setUser_id(Long.parseLong(parseLine[0]));
                bean.setRecipeName("");
                bean.setDate(parseLine[2]);
                bean.setRating(Integer.parseInt(parseLine[3]));
                bean.setFlag("comment");
                outKey.set(Long.parseLong(parseLine[1]));
            } else if(name.startsWith("recipes")){
                bean.setRecipe_id(Long.parseLong(parseLine[1]));
                bean.setUser_id(0);
                bean.setRecipeName(parseLine[0]);
                bean.setDate("");
                bean.setRating(0);
                bean.setFlag("recipe");
                outKey.set(Long.parseLong(parseLine[1]));
            } else return;
        } catch (NumberFormatException e){
            return;
        } catch (ArrayIndexOutOfBoundsException aoe){
            return;
        }

        context.write(outKey, bean);
    }
}
