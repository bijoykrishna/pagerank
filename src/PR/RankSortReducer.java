package PR;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class RankSortReducer extends Reducer<Text, Text, Text, Text> {

    private static Long N;
    public void setup(Context context) throws IOException, InterruptedException {
    	Configuration conf = context.getConfiguration();
    	N = Long.parseLong(conf.get("NumberOfPages"));
    }
    
    public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		final double threshold = 5.0 / N;
		for (Text val : values) {
			String valStr = ((Text)key).toString();
		    if (Double.parseDouble(valStr) >= threshold) {
		    	context.write(val, key);
		    }
		    else {
		    	return;
		    }
		}
    }

}
