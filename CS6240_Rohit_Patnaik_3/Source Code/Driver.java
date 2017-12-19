package cs6240.hw3;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import cs6240.hw3.PageRank.MyPageRankMapper;
import cs6240.hw3.PageRank.MyPageRankReducer;
import cs6240.hw3.TopK.MyTopKMapper;
import cs6240.hw3.TopK.MyTopKReducer;

public class Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		BasicConfigurator.configure();
		Log log = LogFactory.getLog(Driver.class);
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Input/Output Location argument missing");
	      System.exit(2);
	    }
		/* 
		 * Parser Job
		 */
		long parseStartTime = System.currentTimeMillis();
		Job parserJob = Job.getInstance(conf, "Parser");
		parserJob.setJarByClass(Driver.class);
		parserJob.setMapperClass(Bz2WikiParser.class);
		parserJob.setOutputKeyClass(Text.class);
		parserJob.setOutputValueClass(Text.class);
		
		for (int i = 0; i < otherArgs.length - 1; ++i) {
		      FileInputFormat.addInputPath(parserJob, new Path(otherArgs[i]));
		    }
		FileOutputFormat.setOutputPath(parserJob,
			      new Path(otherArgs[otherArgs.length - 1]+ "/" + 0));
		
		//FileInputFormat.addInputPath(parserJob, new Path(args[0]));
		//FileOutputFormat.setOutputPath(parserJob, new Path(args[1] + "/" + 0));
		/* Parser Time*/
		long parseEndTime = System.currentTimeMillis();
		long parseTime = parseEndTime - parseStartTime;
		System.out.println("Parse Time Taken: " + parseTime);
		parserJob.waitForCompletion(true);
		
		
		Counters counters = parserJob.getCounters();
		Counter c1 = counters.findCounter(Node.NUMBER_OF_NODES);
		conf.set(c1.getDisplayName().toString(), Double.toString(c1.getValue()));

		/*
		 * Page Rank Job
		 */
		long pageRankStartTime = System.currentTimeMillis();
		for (int i = 1; i <= 10; i++) {
			Counter c2 = counters.findCounter(Node.PAGERANK_OF_DANGLING_NODES);
			conf.set(c2.getDisplayName().toString(), Double.toString(c2.getValue()));

			Job pageRankJob = Job.getInstance(conf, "PageRank");
			pageRankJob.setJarByClass(Driver.class);

			pageRankJob.setMapperClass(MyPageRankMapper.class);
			pageRankJob.setReducerClass(MyPageRankReducer.class);

			pageRankJob.setMapOutputKeyClass(Text.class);
			pageRankJob.setMapOutputValueClass(Text.class);

			pageRankJob.setOutputKeyClass(Text.class);
			pageRankJob.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(pageRankJob, new Path(otherArgs[otherArgs.length - 1] + "/" + (i - 1)));
			FileOutputFormat.setOutputPath(pageRankJob, new Path(otherArgs[otherArgs.length - 1] + "/" + i));

			pageRankJob.waitForCompletion(true);

		}
		/* pagerank Time*/
		long pageRankEndTime = System.currentTimeMillis();
		long pageRankTime = pageRankEndTime - pageRankStartTime;
		System.out.println("Page Rank Time Taken: " + pageRankTime);
		
		/*
		 * topK Job
		 */
		 long topKStartTime = System.currentTimeMillis();
		Job topKJob = Job.getInstance(conf, "TopK(100)");
		topKJob.setJarByClass(Driver.class);
		topKJob.setMapperClass(MyTopKMapper.class);
		topKJob.setReducerClass(MyTopKReducer.class);
		topKJob.setOutputKeyClass(NullWritable.class);
		topKJob.setOutputValueClass(Text.class);
		topKJob.setNumReduceTasks(1);
		FileInputFormat.addInputPath(topKJob, new Path(otherArgs[otherArgs.length - 1] + "/" + 10));
		FileOutputFormat.setOutputPath(topKJob, new Path(otherArgs[otherArgs.length - 1] + "/top100Pages"));
		System.exit(topKJob.waitForCompletion(true) ? 0 : 1);
		/* topK Time*/
		long topKEndTime = System.currentTimeMillis();
		long topKTime = topKEndTime - topKStartTime;
		System.out.println("Top K Time Taken: " + topKTime);
	}
}
