package PR;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text title = new Text();
    private Text adjacency = new Text();
    private Text adjNode = new Text();
    static int counter = 0;
    private static long nop; //number of pages

    private static String initialized;
    private static String numberOfPages;
    
    
    protected void setup(Context context) throws IOException, InterruptedException {
    	Configuration conf = context.getConfiguration();
    	initialized = conf.get("Initialized");
    	numberOfPages = conf.get("NumberOfPages");
    	nop = Long.parseLong(numberOfPages);    	
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
     	
		String line = value.toString();
		if(line== null){
			return;
		}
		
		if(line.endsWith("\t")) {
		    line = line.substring(0, line.length()-1);
		}
			
		int adjBegin = initialized.equals("false") ? 1 : 2;
		String[] parts = line.split("\t", adjBegin + 1);
		title.set(parts[0]);
	
		if(initialized.equals("true")) {
		    if(parts.length > 2){
		    	String[] adjArray = parts[2].split("\t");
			    adjArray[0] = adjArray[0].substring(1);
			    
			    long adjListSize = adjArray.length;
			    double rank = 0.0;
			    if(adjListSize > 0){
			    	rank = (double)Double.parseDouble(parts[1])/(double)(adjListSize);
			    }
			    
			    for (int i = 0; i < adjListSize; i++) {
			    	adjNode.set(adjArray[i]);
					context.write(adjNode, new Text(Double.toString((double)rank)));
			    }
			    
			    adjacency.set(parts[adjBegin]);
		    }
		    else
		    {
		    	adjacency.set(parts[1]+"#");
		    }
		}
		else
		{
			if(parts.length > 1){
				
				String[] adjArray = parts[1].split("\t");
				
			    long adjListSize = adjArray.length;
			    
			    double rank = 0.0;
			    if(adjListSize > 0){
			    	rank = (double)1/(double)(nop* adjListSize);
			    }
			    
			    for (int i = 0; i < adjListSize; i++) {
			    	adjNode.set(adjArray[i]);
					context.write(adjNode, new Text(Double.toString(rank)));
					
			    }
			    adjacency.set("#" + parts[adjBegin]);
			}
			else
		    {
		    	adjacency.set(0+"#");
		    }		
		}
		
		context.write(title, new Text(adjacency));
    }
}
