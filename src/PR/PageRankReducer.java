package PR;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

    static final double d = 0.85;
    private Text adjacency = new Text();
    private Text outputValue = new Text();

    private static long N;
    private static double factor;
    
    protected void setup(Context context) throws IOException, InterruptedException {
    	Configuration conf = context.getConfiguration();
    	N = Long.parseLong(conf.get("NumberOfPages"));
    	factor = (1 - d) / N;
    }
    
    
    //@Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
	
		double newRank = 0.0;
		adjacency.set("");
		String adjList  = "";
		double rank = 0.0;
		
		for (Text val : values) {
			String valStr = ((Text)val).toString();
			
			if (valStr.startsWith("#")) {
				adjList = new String (valStr);
			}
			else if(valStr.endsWith("#"))
			{
				//do nothing
			}
			else
			{			
				rank += Double.parseDouble(valStr);
			}
		}
		
		newRank = factor + d * rank;		
		String strRank = String.valueOf(newRank);
		
		if(adjList.startsWith("#"))
		{
			outputValue.set(strRank + "\t" + adjList);
		}
		else
		{
			outputValue.set(strRank);
		}
		context.write(key, outputValue);
    }
}
