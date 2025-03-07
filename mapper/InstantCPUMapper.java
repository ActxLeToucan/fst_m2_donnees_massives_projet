package mapper;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class InstantCPUMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
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

        int debut = Integer.parseInt(words[4]);
        int fin = Integer.parseInt(words[5]);

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
        
        for (int i = debut; i < fin; i++) {
            context.write(new Text(i+""), new DoubleWritable(cpuUsage));
        }
    }
}
