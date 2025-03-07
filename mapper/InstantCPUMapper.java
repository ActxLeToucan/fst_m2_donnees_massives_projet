package mapper;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class InstantCPUMapper extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {
	private boolean maxUsage = false;

	@Override 
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		maxUsage = conf.getBoolean("max", false);
	}

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(",", -1);

        long debut = Long.parseLong(words[4]);
        long fin = Long.parseLong(words[5]);

        // 7: moy cpu usage
        // 8: max cpu usage
        int cpuIndex = maxUsage ? 8 : 7;
        if (cpuIndex >= words.length) {
            return;
        }
        String cpuStr = words[cpuIndex];
        if (cpuStr.isEmpty()) {
            return;
        }
        double cpuUsage = Double.parseDouble(cpuStr);
        
        for (long i = debut; i < fin; i++) {
            context.write(new LongWritable(i), new DoubleWritable(cpuUsage));
        }
    }
}
