package driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import java.util.Random ;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import mapper.InstanceToTaskMapper;
import mapper.TaskToJobInstancesMapper;
import reducer.SumReducer;
import reducer.TaskToJobInstancesReducer;

public class JobList extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new JobList(), args);
		System.exit(exitCode);
	}

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.printf("Usage: "+ this.getClass().getName() +" [generic options] <input dir> <output dir> [options]\n");
            return -1 ;
        }

        // job 1 : compter le nombre d'instances par tâche
        Configuration conf = this.getConf();
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(JobList.class);
        job1.setJobName(this.getClass().getName() + ".job1");

        Path tempDir = new Path(this.getClass().getName() + "-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, tempDir);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setMapperClass(InstanceToTaskMapper.class);
        job1.setReducerClass(SumReducer.class);

        job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
        
        boolean success = job1.waitForCompletion(true);
		if (!success) {
			return 1;
		}


        // job 2 : compter le nombre de tâches par job et sommer le nombre d'instances par job
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(JobList.class);
        job2.setJobName(this.getClass().getName() + ".job2");

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.setInputPaths(job2, tempDir);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        job2.setMapperClass(TaskToJobInstancesMapper.class);
        job2.setReducerClass(TaskToJobInstancesReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        success = job2.waitForCompletion(true);

        FileSystem.get(conf).delete(tempDir, true);

        return success ? 0 : 1;
    }
    
}
