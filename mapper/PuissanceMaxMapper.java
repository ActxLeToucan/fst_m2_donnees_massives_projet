package mapper;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PuissanceMaxMapper extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	public void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
		String[] words = key.toString().split(";", -1);
		String machine = words[0];
		context.write(new Text(machine), value);
	}
}
