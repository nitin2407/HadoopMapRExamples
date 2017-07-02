import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingRangeMoviesReducer extends Reducer<Text,Text,Text,Text> {
	
	public void reduce(Text key,Iterable<Text> values,Context context) throws NumberFormatException, IOException, InterruptedException{
		String out=null;
		for(Text value:values){
			out=value.toString();
		}
		context.write(new Text(out.split("~")[1]),new Text(out.split("~")[0]));
	}

}
