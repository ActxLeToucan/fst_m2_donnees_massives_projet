package reducer;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double max = Double.MIN_VALUE;
		for (DoubleWritable value : values) {
			max = Math.max(max, value.get());
		}
		context.write(key, new DoubleWritable(max));
	}
}
