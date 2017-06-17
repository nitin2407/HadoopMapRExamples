

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class NetflixYearDataCountAnalysis {
	

	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		if(args.length!=2){
			System.err.println("Usage: MaxTemperature <input path> <output path>");
			System.exit(-1);
		}
		
		Job job = Job.getInstance();
		job.setJarByClass(NetflixYearDataCountAnalysis.class);
		job.setJobName("Max Temperature");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(YearDataMapper.class);
		job.setReducerClass(YearDataReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
		
	}
	
}
