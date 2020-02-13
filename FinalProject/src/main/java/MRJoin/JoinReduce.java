package MRJoin;

import Bean.JoinBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinReduce extends Reducer<LongWritable, JoinBean, JoinBean, NullWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {
        List<JoinBean> list = new ArrayList<JoinBean>();
        JoinBean recipeBean = new JoinBean();

        for(JoinBean val : values){
            if(val.getFlag().equals("comment")) {
                JoinBean commentBean = new JoinBean();
                try{
                    BeanUtils.copyProperties(commentBean, val);
                    list.add(commentBean);
                } catch (Exception e){
                    e.printStackTrace();
                }
            }else {
                try{
                    BeanUtils.copyProperties(recipeBean, val);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        for(JoinBean bean : list) {
            bean.setRecipeName(recipeBean.getRecipeName());
            context.write(bean, NullWritable.get());
        }
    }

}
