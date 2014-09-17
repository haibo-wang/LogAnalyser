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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LogAnalyser extends Configured implements Tool {

	private LogAnalyser() {
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("LogAnalyser <inDir> <outDir>");
			ToolRunner.printGenericCommandUsage(System.out);
			return 2;
		}

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "log-analysis");

		NonSplitableTextInputFormat.setInputPaths(job, args[0]);
		job.setInputFormatClass(NonSplitableTextInputFormat.class);
		
		job.setMapperClass(RegexFilenameMapper.class);
		job.setNumReduceTasks(0); // we do not need any reducer job
		

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static class RegexFilenameMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private String fileName;
		private Pattern badLinesPattern = Pattern.compile("\\b([1-9]\\d*)\\s+bad\\s+lines\\b");

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
			if ( matcher.find() ) {
				context.write(new Text(fileName), new IntWritable(Integer.parseInt(matcher.group(1))));
			}
		}

	}

	// each log file is a atomic unit. we do not want it to be split
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
