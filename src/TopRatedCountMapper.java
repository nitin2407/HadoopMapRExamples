import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopRatedCountMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	NetflixDataParser parser = new NetflixDataParser();
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		
		parser.parse(value.toString());
		String out = parser.getMovieName() + "~" + parser.getRating();
		if(parser.isRatingValid() && !parser.isHeader()){
			context.write(new Text("one"), new Text(out));
		}
		
	}

}
