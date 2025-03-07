package driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.Random;
import mapper.InstantCPUMapper;
import reducer.DoubleSumReducer;

public class Pic extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new Pic(), args);
		System.exit(exitCode);
	}

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.printf("Usage: "+ this.getClass().getName() +" [generic options] <input dir> <output dir> [options]\n");
            return -1 ;
        }

        Configuration conf = this.getConf();
        conf.setBoolean("max", false) ;

        int i = 2;
		while(i < args.length) {
			if (args[i].equals("-max")) {
				conf.setBoolean("max", true);
			} else {
				System.out.printf("Unknown option "+args[2]+"\n");
				System.exit(-1);
			}
			i++;
		}

        // job 1 : sommer l'usage à chaque instant
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(JobList.class);
        job1.setJobName(this.getClass().getName() + ".job1");

        Path tempDir = new Path(this.getClass().getName() + "-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, tempDir);
		//job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setMapperClass(InstantCPUMapper.class);
        job1.setCombinerClass(DoubleSumReducer.class);
        job1.setReducerClass(DoubleSumReducer.class);

        job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(DoubleWritable.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
        
        boolean success = job1.waitForCompletion(true);
		if (!success) {
			return 1;
		}

        return 0;
    }
}
