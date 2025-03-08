package mapper;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InstantCPUMachineMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(",", -1);

        long debut = Long.parseLong(words[4]);
        long fin = Long.parseLong(words[5]);

        // 8: max cpu usage
        int cpuIndex = 8;
        if (cpuIndex >= words.length) {
            return;
        }
        String cpuStr = words[cpuIndex];
        if (cpuStr.isEmpty()) {
            return;
        }
        double cpuUsage = Double.parseDouble(cpuStr);
        
        for (long i = debut; i < fin; i++) {
            // key: machine;instant
            // value: max cpu usage
            context.write(new Text(words[6]+";"+i), new DoubleWritable(cpuUsage));
        }
    }
}
