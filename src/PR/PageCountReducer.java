package PR;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class PageCountReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
    	
		long sum = 0;
		for (Text txt : values) {
			sum += 1;
		}
		context.write(new Text("N=" + sum), null);
    }

}
