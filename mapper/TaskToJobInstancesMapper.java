package mapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import java.util.*;

public class TaskToJobInstancesMapper extends Mapper<Text, IntWritable, Text, IntWritable> {

    /**
     * key : job;task
     * value : nb_instance
     * 
     * write: job, nb_instance
     */
    @Override
    public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        String[] words = key.toString().split(";");
        context.write(new Text(words[0]), value);
    }
}
