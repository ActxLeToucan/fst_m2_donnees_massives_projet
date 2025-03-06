package reducer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TaskToJobInstancesReducer extends Reducer<Text, IntWritable, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int instances = 0;
        int tasks = 0;
        for (IntWritable value : values) {
            instances += value.get();
            tasks++;
        }
        context.write(key, new Text(tasks + " ; " + instances));
	}
}
