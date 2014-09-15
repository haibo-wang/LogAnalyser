package net.haibo.loganalyser;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogAnalyser extends Configured implements Tool

{
	public static class RegexFilenameMapper extends Mapper<Object, Text, Text, IntWritable> {
		
	}
	
	
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
    }

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
}
