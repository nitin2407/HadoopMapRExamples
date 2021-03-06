import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NetflixTopRatedMovies {

public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		if(args.length!=2){
			System.err.println("Usage: Netflix <input path> <output path>");
			System.exit(-1);
		}
		
		Job job = Job.getInstance();
		job.setJarByClass(NetflixTopRatedMovies.class);
		job.setJobName("Netflix top rated movies");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(TopRatedCountMapper.class);
		job.setReducerClass(TopRatedCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
		
	}
}
