package PR;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank {
    private static String numberOfPages;
    private static String initialized = "false";
       
    // Job1: Count number of pages
    public static void calTotalPages(String input, String output) throws Exception {
    	Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Page Count");
		job.setJarByClass(PageCountMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(PageCountMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageCountReducer.class);
        
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }    

    // Job2: Calculate PageRank
    public static boolean calPageRank(String input, String output) throws Exception {
    	Configuration conf = new Configuration();		
        conf.set("NumberOfPages", numberOfPages);
        conf.set("Initialized", initialized);

        Job job = Job.getInstance(conf, "Page Rank");
		job.setJarByClass(PageRankMapper.class);
		
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);        
        
        job.setMapperClass(PageRankMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankReducer.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
        
        return true;
    }

    public static class DescDoubleComparator extends WritableComparator {

        protected DescDoubleComparator() {
	        super(Text.class, true);
	        }
	
	        @SuppressWarnings("rawtypes")
	        @Override
	        public int compare(WritableComparable w1, WritableComparable w2) {
	        	
	        String s1 = ((Text)w1).toString();
	        double d1 = Double.parseDouble(s1);
		    
		    String s2 = ((Text)w2).toString();
		    double d2 = Double.parseDouble(s2);
		    
	        return (d1 > d2 ? -1 : (d1 == d2 ? 0 : 1));
        }
    }

    // Job 3: Sort by PageRank and write out Pages >= 5/N
    public static void orderRank(String input, String output) throws Exception {
    	Configuration conf = new Configuration();
        conf.set("NumberOfPages", numberOfPages);

        Job job = Job.getInstance(conf, "Rank Sort");
		job.setJarByClass(RankSortMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(RankSortMapper.class);
        job.setSortComparatorClass(DescDoubleComparator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(RankSortReducer.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        job.waitForCompletion(true);
    }

	public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        
        Path inLink = new Path(args[1]+"/temp/num_Res");
        FileSystem fs = inLink.getFileSystem(conf);
	    
        PageRank.calTotalPages(args[0], args[1]+"/temp/num_Res");
        FileUtil.copyMerge(fs, inLink, fs, new Path(args[1]+"/num_nodes"), false, conf, "");
        
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(args[1]+"/num_nodes"))));        
        String line = br.readLine();        
        String nop = null;
        long totalPages = 0;
        
        while(line != null){
        	nop = line.substring(line.indexOf('=') + 1);
        	totalPages += Long.parseLong(nop);        	
        	line = br.readLine();
        }
        
        numberOfPages = Long.toString(totalPages);       
        
        PageRank.calPageRank(args[0], args[1]+"/temp/iter1");
        
        initialized = "true";
        int count = 1;
        
        for (count = 1; count< 8; count++){
        	PageRank.calPageRank(args[1]+"/temp/iter" + count, args[1]+"/temp/iter" + (count + 1));
        }
        
        int count8 = 8;
        int count1 = 1;

        PageRank.orderRank(args[1]+"/temp/iter" + count1, args[1]+"/temp/sort/sort"+count1);
        FileUtil.copyMerge(fs, new Path(args[1] + "/temp/sort/sort"+count1), fs, new Path(args[1] + "/iter1.out"), false, conf, "");
        
        PageRank.orderRank(args[1]+"/temp/iter" + count8, args[1]+"/temp/sort/sort"+count8);
        FileUtil.copyMerge(fs, new Path(args[1] + "/temp/sort/sort"+count8), fs, new Path(args[1] + "/iter8.out"), false, conf, "");        
        
        System.exit(0);
    }
}


