package MRJoin;

import Bean.JoinBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

public class JoinJob {
    public static void main(String[] args){
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        try{
            Job job = Job.getInstance(conf,"JoinTwoTable");
            job.setJarByClass(JoinJob.class);

            //Set mapper and reducer
            job.setMapperClass(JoinMap.class);
            job.setReducerClass(JoinReduce.class);

            //Set InputFormat and OutputFormat
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(JoinBean.class);

            //Set output key and value types
            job.setOutputKeyClass(JoinBean.class);
            job.setOutputValueClass(NullWritable.class);

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
