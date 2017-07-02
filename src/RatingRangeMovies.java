import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RatingRangeMovies {
public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		if(args.length!=2){
			System.err.println("Usage: Netflix <input path> <output path>");
			System.exit(-1);
		}
		
		Job job = Job.getInstance();
		job.setJarByClass(RatingRangeMovies.class);
		job.setJobName("Movies divided by range.");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setPartitionerClass(RatingRangeMoviesPartitioner.class);
		job.setMapperClass(RatingRangeMoviesMapper.class);
		job.setReducerClass(RatingRangeMoviesReducer.class);
		job.setNumReduceTasks(3);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
		
	}

}
