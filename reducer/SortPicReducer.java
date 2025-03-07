package reducer;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import writable.PicWritable;

public class SortPicReducer<V> extends Reducer<PicWritable, V, LongWritable, Text> {
	@Override
	public void reduce(PicWritable key, Iterable<V> values, Context context) throws IOException, InterruptedException {
		context.write(new LongWritable(key.getInstant()), new Text(key.getUsage() + ";" + key.getDuration()));
	}
}
