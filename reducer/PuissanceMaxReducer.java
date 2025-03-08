package reducer;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PuissanceMaxReducer extends Reducer<Text, DoubleWritable, Text, LongWritable> {
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double max = Double.MIN_VALUE;
		for (DoubleWritable value : values) {
			max = Math.max(max, value.get());
		}
		long nbCore = Math.round(Math.ceil(max/100.0));
		context.write(key, new LongWritable(nbCore));
	}
}
