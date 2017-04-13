package PR;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class PageCountMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text one = new Text("1");
    private Text word = new Text("");

    @Override
    public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
    	
    	context.write(one, word);
    }

}
