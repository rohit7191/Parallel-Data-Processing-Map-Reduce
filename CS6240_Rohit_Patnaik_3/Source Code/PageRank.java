package cs6240.hw3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class PageRank {
	static double alpha = 0.15;
	//Mapper Class
	public static class MyPageRankMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			/* 
			 * Getting values from enum
			 */
			Configuration conf = context.getConfiguration();
			double numNodes = Double.parseDouble(conf.get("NUMBER_OF_NODES"));
			double prOfDanglingNodes = Double.parseDouble(conf.get("PAGERANK_OF_DANGLING_NODES"));

			String line = value.toString().replaceAll(" ", "");
			String[] splitArr = line.split(":");
			String page = splitArr[0].trim();
			double pageRank = 0.0, deltaPageRank = 0.0;
			
			/*
			 * Calculating the Page Rank value of the page. The first iteration will encounter value field as DummyPageRankValue
			 */
			if (splitArr[1].equals("DummyPageRankValue")) {
				pageRank = 1.0 / numNodes;
			} else {
				pageRank = Double.parseDouble(splitArr[1]);
			}
			
			/*
			 * Calculating the deltaPageRank
			 */
			 
			deltaPageRank = (1 - alpha) * prOfDanglingNodes / numNodes;
			
			/*
			 * Calculating the new page rank
			 */
			pageRank = pageRank + deltaPageRank;

			String adjacentList = splitArr[2];
			/*
			 * Extracting the adjacent pages of the current page and calculating their page rank and emitting it
			 */
			if (!adjacentList.equals("[]")) {

				adjacentList = adjacentList.substring(1, adjacentList.length() - 1);

				String[] outPages = adjacentList.split(",");
				double numOfOutlinks = outPages.length;

				for (String p : outPages) {
					Text outPageKey = new Text(p);
					Text outPageRankValue = new Text(Double.toString((pageRank / numOfOutlinks)));

					context.write(outPageKey, outPageRankValue);
				}
			}

			String outputAdjacentList = splitArr[2];
			Text outKey = new Text(page);
			Text outVal = new Text(outputAdjacentList);
			
			/*Emitting page and its adjacency list*/
			context.write(outKey, outVal);
			/*Emitting dangling pages*/
			context.write(outKey, new Text("&"));

		}
	}

	public static class MyPageRankReducer extends Reducer<Text, Text, Text, Text> {
		// Reducer Class
		Text outVal;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			/* 
			 * Getting values from enum
			 */
			Configuration conf = context.getConfiguration();
			double numNodes = Double.parseDouble(conf.get("NUMBER_OF_NODES"));

			int flagWiki = 0, flagNode = 0;
			double currPageRank = 0.0;
			
			/*
			 * Summing total page rank of the same key
			 */
			for (Text value : values) {
				String line = value.toString();

				if (line.equals("&")) {
					flagWiki = 1;
					continue;
				}
				if (line.charAt(0) == '[') {
					/* Check for Dangling Node*/
					if (line == "[]") {
						flagNode = 1;
					}
					outVal = new Text(line);
					continue;
				}
				currPageRank += Double.parseDouble(line);
			}

			if (flagWiki == 0) {
				return;
			}
			/* 
			 * Updating the total page rank 
			 */
			double outPageRankValue = (1 - alpha) * currPageRank + (alpha / numNodes);
			context.write(key, new Text(":" + outPageRankValue + ":" + outVal));

			if (flagNode == 1) {
				/*
				 * Incrementing the pagerank value to the dangling node contribution for next iteration
				 */
				context.getCounter(Node.PAGERANK_OF_DANGLING_NODES)
						.increment((long) (outPageRankValue));
			}
		}
	}
}
