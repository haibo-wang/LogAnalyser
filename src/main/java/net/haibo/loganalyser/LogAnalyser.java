package net.haibo.loganalyser;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
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

		
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "log-analysis");

		job.setJarByClass(LogAnalyser.class);

		NonSplitableTextInputFormat.setInputPaths(job, args[0]);
		job.setInputFormatClass(NonSplitableTextInputFormat.class);
		job.setMapperClass(RegexFilenameMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		

		job.setReducerClass(Reducer.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(1); 

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	
	// As now we use CombineTextInputFormat, (usually) multiple files will be combined into
	// a split. We cannot just simply break the loop in run() method when we find the bad pattern
	// -- that will break out of the map for multiple files. Use Combiner/Reducer to do the trick
	public static class RegexFilenameMapper extends
			Mapper<LongWritable, Text, Text, NullWritable> {

	
		private Pattern badLinesPattern = Pattern
				.compile("\\b([1-9]\\d*)\\s+bad\\s+lines\\b");
		private int idx = -1; // index into the set of files in the combined file
		

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			if ( key.get() == 0) {
				idx++;
			}
			
			Matcher matcher = badLinesPattern.matcher(value.toString());
			if (matcher.find()) {
				String fileName = ((CombineFileSplit) context.getInputSplit()).getPath(idx).getName();
				//String fileName = context.getConfiguration().get(MRJobConfig.MAP_INPUT_FILE);
				context.write(new Text(fileName),
						NullWritable.get());
			}
		}
	}
	

	public static class IdentityReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {


				context.write(key, NullWritable.get());
		}
	}

	// each log file is an atomic unit. Splitting it has no meaning here.
	public static class NonSplitableTextInputFormat extends CombineTextInputFormat {

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
