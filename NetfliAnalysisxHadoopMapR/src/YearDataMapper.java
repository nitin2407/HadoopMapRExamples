import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YearDataMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

	private NetflixDataParser netflixDataParser = new NetflixDataParser(); 

	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		netflixDataParser.parse(value.toString());
		String year = netflixDataParser.getYear();
		if(!netflixDataParser.isHeader() && netflixDataParser.isYearValid()){
			context.write(new Text(year),new IntWritable(1));
		}
	}
}
