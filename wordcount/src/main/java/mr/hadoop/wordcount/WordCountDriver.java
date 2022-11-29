package mr.hadoop.wordcount;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration config = new Configuration();
		
		JobConf jobConf = new JobConf(config, WordCountDriver.class);

		Path inputPath = new Path(args[0]);
    	FileInputFormat.setInputPaths(jobConf, inputPath);

    	Path outputPath = new Path(args[1]);
    	FileOutputFormat.setOutputPath(jobConf, outputPath);
		outputPath.getFileSystem(config).delete(outputPath, true);

		Job job = getJob(jobConf);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	private Job getJob(JobConf jobConfig) throws IOException {
		Job job = Job.getInstance(jobConfig, "MR Job - Word Count");

		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WordCountDriver(), args);
        System.exit(exitCode);
    }
}
