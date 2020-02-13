package MRCommentUser;

import Bean.CommentBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class UserIDJob {
    public static void main(String[] args){
        //hdfs://localhost:9000/
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        try{
            Job job = Job.getInstance(conf,"UserIDMR");
            job.setJarByClass(UserIDJob.class);

            //Set mapper and reducer
            job.setMapperClass(UserIDMap.class);
            job.setReducerClass(UserIDReduce.class);

            //Set InputFormat and OutputFormat
            //job.setInputFormatClass(TextInputFormat.class);
            //job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(CommentBean.class);

            //Set output key and value types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //Set the input and out path
            FileInputFormat.addInputPath(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0:1);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
