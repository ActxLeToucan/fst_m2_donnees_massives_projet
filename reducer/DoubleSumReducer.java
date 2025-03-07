package reducer;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DoubleSumReducer extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {
	@Override
	public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double sum = 0;
		for (DoubleWritable value : values) {
			sum += value.get();
		}
		context.write(key, new DoubleWritable(sum));
	}
}
