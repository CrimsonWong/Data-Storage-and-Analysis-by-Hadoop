package MRCommentRecipe;

import Bean.CommentBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;


import java.io.IOException;

public class RecipeIDJob {
    public static void main(String[] args){
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        try{
            //job1
            Job job1 = Job.getInstance(conf,"RecipeIDMR");
            job1.setJarByClass(RecipeIDJob.class);

            //Set mapper and reducer
            job1.setMapperClass(RecipeIDMap.class);
            job1.setReducerClass(RecipeIDReduce.class);

            //Set map output key and value
            job1.setMapOutputKeyClass(LongWritable.class);
            job1.setMapOutputValueClass(CommentBean.class);

            //Set output key and value types
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            //Add job1 to controller
            ControlledJob ctrljob1 = new ControlledJob(conf);
            ctrljob1.setJob(job1);

            //Set the input and out path
            FileInputFormat.addInputPath(job1,new Path(args[0]));
            FileOutputFormat.setOutputPath(job1,new Path(args[1]));

            //job2
            Job job2 = Job.getInstance(conf,"RecipeIDRatingMR");
            job2.setJarByClass(RecipeIDJob.class);

            //Set mapper and reducer
            job2.setMapperClass(RecipeIDMap.class);
            job2.setReducerClass(RecipeRatingReduce.class);

            //Set map output key and value
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(CommentBean.class);

            //Set output key and value types
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);

            //Set Partitioner Class
            job2.setPartitionerClass(RatingPartitioner.class);
            job2.setNumReduceTasks(6);

            //Set the input and out path
            FileInputFormat.addInputPath(job2,new Path(args[0]));
            FileOutputFormat.setOutputPath(job2,new Path(args[1]));

            //Add job2 to controller
            ControlledJob ctrljob2 = new ControlledJob(conf);
            ctrljob2.setJob(job2);

            //Add dependency
            ctrljob2.addDependingJob(ctrljob1);

            //Set the input and out path
            FileInputFormat.addInputPath(job2,new Path(args[2]));
            FileOutputFormat.setOutputPath(job2,new Path(args[3]));

            //Add Main Controller
            JobControl jobCtrl = new JobControl("myOutCount");
            jobCtrl.addJob(ctrljob1);
            jobCtrl.addJob(ctrljob2);

            //Thread Run
            Thread t = new Thread(jobCtrl);
            t.start();
            while (true) {
                if (jobCtrl.allFinished()) {
                    System.out.println(jobCtrl.getSuccessfulJobList());
                    jobCtrl.stop();
                    break;
                }
            }

            System.exit(job1.waitForCompletion(true) ? 0:1);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();

        }
    }
}
