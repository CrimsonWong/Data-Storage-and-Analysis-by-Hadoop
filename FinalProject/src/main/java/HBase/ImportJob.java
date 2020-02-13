package HBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class ImportJob extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        // 实例化job
        Job job = Job.getInstance(this.getConf());
        // 设置jar包路径
        job.setJarByClass(ImportJob.class);

        // 组装mapper
        job.setMapperClass(ImportMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置数据来源
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/FinalProject/JoinComment.csv"));

        // 组装reducer
        TableMapReduceUtil.initTableReducerJob("finalProject", ImportReduce.class, job);

        // 设置reduce个数
        job.setNumReduceTasks(1);

        // 提交
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        BasicConfigurator.configure();
        Configuration conf = HBaseConfiguration.create();
        ToolRunner.run(new ImportJob(), args);
    }
}

