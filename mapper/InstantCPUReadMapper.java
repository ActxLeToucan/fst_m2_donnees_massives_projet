package mapper;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class InstantCPUReadMapper extends Mapper<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {
    @Override
    public void map(LongWritable key, DoubleWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
