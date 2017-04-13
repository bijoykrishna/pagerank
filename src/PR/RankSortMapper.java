package PR;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class RankSortMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text rank; 
    private Text title = new Text();

    public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] s = line.split("\t", 3);
		title.set(s[0]);
		rank = new Text(s[1]);
		context.write(rank, title);
    }

}
