package MRCommentRecipe;

import Bean.CommentBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class RatingPartitioner extends Partitioner<LongWritable, CommentBean> {
    @Override
    public int getPartition(LongWritable text, CommentBean comment, int i) {
        int partition = 0;
        if (comment.getRating() == 5) partition = 0;
        else if (comment.getRating() == 4) partition = 1;
        else if (comment.getRating() == 3) partition = 2;
        else if (comment.getRating() == 2) partition = 3;
        else if (comment.getRating() == 1) partition = 4;
        else partition = 5;
        return partition;
    }
}
