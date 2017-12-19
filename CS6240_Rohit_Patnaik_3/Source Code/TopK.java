package cs6240.hw3;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class TopK {
	static double alpha = 0.15;
	static int k = 100;
	//Mapper Class
	public static class MyTopKMapper extends Mapper<Object, Text, NullWritable, Text> {

		Map<String, Double> hm;
		
		protected void setup(Context context)
				throws IOException, InterruptedException {
			/*
			 * Initializing HashMap
			 */
				hm =  new HashMap<String, Double>();
				}

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
			 * Calculating the Page Rank value of the page. The first iteration might encounter value field as DummyPageRankValue though it is highly unlikely
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
			
			/* Storing it in a HashMap */
			
			hm.put(page, pageRank);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {

			hm = (HashMap<String, Double>) MySort.sortByValue(hm);
			int counter = 1;
			/*
			 * Emitting only first 100 pages and its rank after sorting.
			 */
			for (String key : hm.keySet()) {
				if (counter <= k) {
					context.write(NullWritable.get(), new Text(key + ":" + hm.get(key)));
					counter++;
				} else
					break;
			}
		}
	}

	public static class MySort {

		public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {

			List<Map.Entry<K, V>> list = new ArrayList<Map.Entry<K, V>>(map.entrySet());
			Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
				public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
					return -1 * (o1.getValue()).compareTo(o2.getValue());
				}
			});

			Map<K, V> sortedMap = new LinkedHashMap<K, V>();
			for (Map.Entry<K, V> entry : list) {
				sortedMap.put(entry.getKey(), entry.getValue());
			}

			return sortedMap;
		}
	}

	public static class MyTopKReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
		// Reducer Class
		public void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			Map<String, Double> topKMap = new HashMap<String, Double>();
			/*
			 * For every page, putting it in Linked HashMap
			 */
			for (Text value : values) {
				String line = value.toString();
				String[] splitArr = line.split(":");
				String key1 = splitArr[1];
				double pageRank = Double.parseDouble(key1);
				topKMap.put(line, pageRank);

			}

			topKMap = (HashMap<String, Double>) MySort.sortByValue(topKMap);
			int count = 1;

			for (String pr : topKMap.keySet()) {

				if (count <= k) {
					context.write(NullWritable.get(), new Text(pr));
					count++;
				} else
					break;
			}
		}
	}
}
