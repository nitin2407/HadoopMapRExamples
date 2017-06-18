import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopRatedCountReducer extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		String outValue[];
		//TreeMap<Integer,String> ratings = new TreeMap<>();
		
		ArrayList<String> ratings = new ArrayList<>();
		for(Text in: values){
			//outValue = in.toString().split("~");
			ratings.add(in.toString());
		}
		ratings = removeDups(ratings);
		outValue = new String[ratings.size()];
		outValue = sortValues(ratings.toArray(outValue));
		/*for(Text in: values){
			outValue= in.toString().split("~");
			ratings.put(Integer.parseInt(outValue[1]),outValue[0]);
		}
		NavigableMap<Integer,String> sortedRatings = ratings.descendingMap();
		StringBuilder topMovies = new StringBuilder("|");
		int count=1;
		for(int rating:sortedRatings.keySet()){
			topMovies.append(sortedRatings.get(rating) + "-" + rating + "|"); 
			count++;
			if(count>10){
				break;
			}
		}
		*/
		StringBuilder topMovies = new StringBuilder("|");
		int count=1;
		for(String record:outValue){
			topMovies.append(record + "|"); 
			count++;
			if(count>10){
				break;
			}
		}
		context.write(new Text("Top Rated Movies: "), new Text(topMovies.toString()));
		
	}
	
	public static ArrayList<String> removeDups(ArrayList<String> input){
		HashMap<String,String> unique = new HashMap<>();
		ArrayList<String> output = new ArrayList<>();
		String arr[];
		for(String record:input){
			arr = record.split("~");
			unique.put(arr[0], arr[1]);
		}
		for(String record:unique.keySet()){
			output.add(record + "~" + unique.get(record)); 
		}
		return output;
	}
	public static void merge(String arr[], int l, int m, int r)
    {
      
        int n1 = m - l + 1;
        int n2 = r - m;
 
        String L[] = new String [n1];
        String R[] = new String [n2];
 
        for (int i=0; i<n1; ++i)
            L[i] = arr[l + i];
        for (int j=0; j<n2; ++j)
            R[j] = arr[m + 1+ j];
 
 
        int i = 0, j = 0;
        int k = l;
        while (i < n1 && j < n2)
        {
            if (Integer.parseInt(L[i].split("~")[1]) > Integer.parseInt(R[j].split("~")[1]))
            {
                arr[k] = L[i];
                i++;
            }
            else
            {
                arr[k] = R[j];
                j++;
            }
            k++;
        }
 
        while (i < n1)
        {
            arr[k] = L[i];
            i++;
            k++;
        }
        while (j < n2)
        {
            arr[k] = R[j];
            j++;
            k++;
        }
    }
    public static void sort(String arr[], int l, int r)
    {
        if (l < r)
        {
            int m = (l+r)/2;
            sort(arr, l, m);
            sort(arr , m+1, r);
            merge(arr, l, m, r);
        }
    }
    
	public static String[] sortValues(String values[]){
		sort(values, 0, values.length - 1);
		return values;
	}
}

