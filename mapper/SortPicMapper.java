package mapper;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import writable.PicWritable;

public class SortPicMapper extends Mapper<LongWritable, Text, PicWritable, NullWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(new PicWritable(key.get(), value), NullWritable.get());
	}
}
