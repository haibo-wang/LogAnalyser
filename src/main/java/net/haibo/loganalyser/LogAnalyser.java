package net.haibo.loganalyser;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author haibo
 *
 *         Given the log files in a input dir, find out those containing
 *         "xxx bad lines" and write out the file name together with the xxx
 *         (the number of bad lines)
 */

public class LogAnalyser extends Configured implements Tool {

	// prevent multiple instances of it
	private LogAnalyser() {
	}

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length < 2) {
			System.out.println("LogAnalyser <inDir> <outDir>");
			ToolRunner.printGenericCommandUsage(System.out);
			return 2;
		}


		System.out.println(String.format(
				"Running LogAnalyser with input %s and output %s", args[0],
				args[1]));
		
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "log-analysis");

		job.setJarByClass(LogAnalyser.class);

		NonSplitableTextInputFormat.setInputPaths(job, args[0]);
		job.setInputFormatClass(NonSplitableTextInputFormat.class);
		job.setMapperClass(RegexFilenameMapper.class);

		job.setReducerClass(IdentityReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(1); // we do not need any reducer job

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static class RegexFilenameMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private String fileName;
		private Pattern badLinesPattern = Pattern
				.compile("\\b([1-9]\\d*)\\s+bad\\s+lines\\b");

		@Override
		protected void setup(
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			super.setup(context);

			fileName = context.getConfiguration().get(
					MRJobConfig.MAP_INPUT_FILE);

		}

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			super.map(key, value, context);

			Matcher matcher = badLinesPattern.matcher(value.toString());
			if (matcher.find()) {
				context.write(new Text(fileName),
						new IntWritable(Integer.parseInt(matcher.group(1))));
			}
		}

	}

	// the default implementation will just write out whatever it gets from the
	// map task
	public static class IdentityReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

	}

	// each log file is an atomic unit. Splitting it has no meaning here.
	public static class NonSplitableTextInputFormat extends TextInputFormat {

		@Override
		protected boolean isSplitable(JobContext context, Path file) {
			return false;
		}

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new LogAnalyser(), args);
		System.exit(res);

	}

}
