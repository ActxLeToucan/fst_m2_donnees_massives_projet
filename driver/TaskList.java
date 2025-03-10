package driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import reducer.TaskListReducer;
import mapper.TaskListMapper;


public class TaskList extends Configured implements Tool {

	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new Configuration(), new TaskList(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: TaskList [generic options] <input dir> <output dir> [options]\n");
			return -1 ;
		}

		Configuration conf = this.getConf() ;
	
		Job job = Job.getInstance(conf);

		job.setJarByClass(TaskList.class);
		job.setJobName("Task-List");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(TaskListMapper.class);
		job.setReducerClass(TaskListReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		if(success){
			return 0 ;
		}else{
			return -1 ;
		}
	}
}






