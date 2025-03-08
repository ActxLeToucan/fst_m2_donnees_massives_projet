package driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.Random;
import mapper.InstantCPUMapper;
import mapper.InstantCPUReadMapper;
import mapper.SortPicMapper;
import reducer.LongDoubleSumReducer;
import reducer.PicDetectionReducer;
import reducer.SortPicReducer;
import writable.PicWritable;

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
        job1.setJarByClass(Pic.class);
        job1.setJobName(this.getClass().getName() + ".job1");

        Path tempDir1 = new Path(this.getClass().getName() + ".1-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, tempDir1);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setMapperClass(InstantCPUMapper.class);
        job1.setCombinerClass(LongDoubleSumReducer.class);
        job1.setReducerClass(LongDoubleSumReducer.class);

        job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(DoubleWritable.class);

		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(DoubleWritable.class);
        
        boolean success = job1.waitForCompletion(true);
		if (!success) {
			return 1;
		}


        // job 2 : detecter les pics
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(Pic.class);
        job2.setJobName(this.getClass().getName() + ".job2");

        job2.setNumReduceTasks(1);

        Path tempDir2 = new Path(this.getClass().getName() + ".2-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.setInputPaths(job2, tempDir1);
        FileOutputFormat.setOutputPath(job2, tempDir2);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        job2.setMapperClass(InstantCPUReadMapper.class);
        job2.setReducerClass(PicDetectionReducer.class);

        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(DoubleWritable.class);

        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);

        success = job2.waitForCompletion(true);

        FileSystem.get(conf).delete(tempDir1, true);

        if (!success) {
			return 1;
		}


        // job 3 : ordonner les pics
        Job job3 = Job.getInstance(conf);
        job3.setJarByClass(Pic.class);
        job3.setJobName(this.getClass().getName() + ".job3");

        job3.setNumReduceTasks(1);

        job3.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.setInputPaths(job3, tempDir2);
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));

        job3.setMapperClass(SortPicMapper.class);
        job3.setReducerClass(SortPicReducer.class);

        job3.setMapOutputKeyClass(PicWritable.class);
        job3.setMapOutputValueClass(NullWritable.class);

        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(Text.class);

        success = job3.waitForCompletion(true);

        FileSystem.get(conf).delete(tempDir2, true);

        return success ? 0 : 1;
    }
}
