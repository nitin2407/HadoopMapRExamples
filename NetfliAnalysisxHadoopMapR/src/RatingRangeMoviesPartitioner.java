import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RatingRangeMoviesPartitioner extends Partitioner<Text,Text> {

	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		
		int rating = Integer.parseInt(value.toString().split("~")[1]);
		if(numReduceTasks==0){
			return 0;
		}
		if(rating<80){
			return 0;
		}
		else if(rating>=80 && rating<90){
			return 1%numReduceTasks;
		}
		else if(rating>=90){
			return 2%numReduceTasks;
		}
		return 0;
	}

}
