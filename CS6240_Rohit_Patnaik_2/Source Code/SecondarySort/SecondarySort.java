package hw2.cs6240;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class SecondarySort {

	public static class MySecondarySortMapper extends Mapper<Object, Text, Text, Text> {
		// Mapper class writes the key and Text to the Context	
		// Key is combination of year and stationID and value is Text which is a combination of type and temperature nd year	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Text outVal;
			Text outKey;
			String line = value.toString();
			String[] splitArr = line.split(",");
			if (splitArr[2].equals("TMAX") || splitArr[2].equals("TMIN")) {
				Text stationID = new Text(splitArr[0]);
				Text year = new Text(new String(splitArr[1].substring(0,4)));
				Text type = new Text(splitArr[2]);
				Text temperature = new Text((splitArr[3]));
				outKey = new Text(year + "," + stationID);
				outVal = new Text(type + "," + temperature + "," + year);
				context.write(outKey, outVal);
			}
		}
	}
	public static class MySecondarySortPartitioner extends Partitioner<Text, Text> {
		// Partitioner - partitions as per the hashcode of stationID to determine the reducer call
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String[] keyArr = key.toString().split(",");
			Text stationID = new Text(keyArr[1]);
			return Math.abs((stationID.hashCode() % numReduceTasks));
		}
	}

public static class MyKeySortComparator extends WritableComparator{
		// Key is sorted based on station IDs. If the stationID matches, then the years are compared and sorted.
		protected MyKeySortComparator() {
			super(Text.class, true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2){
			Text key1 = (Text) w1;
			Text key2 = (Text) w2;
			
			String[] key1Arr = key1.toString().split(",");
			String year1 = key1Arr[0];
			String sid1 = key1Arr[1];
			
			String[] key2Arr = key2.toString().split(",");
			String year2 = key2Arr[0];
			String sid2 = key2Arr[1];

			// Compare both station ID and year
			int cmp = sid1.compareTo(sid2);
			if(cmp == 0){
				return year1.compareTo(year2);
			}
			return cmp;
		}
}

public static class MySecondarySortGroupingComparator extends WritableComparator {
	// Group Sort Comparator sorts on basis of stationID as years are combined per stationID
	protected MySecondarySortGroupingComparator(){
		super(Text.class,true);
	}

	public int compare(WritableComparable w1, WritableComparable w2){
		
		Text key1 = (Text) w1;
		Text key2 = (Text) w2;
		String[] key1Arr = key1.toString().split(",");
		String[] key2Arr = key2.toString().split(",");
		
		String sid1 = key1Arr[1];
		String sid2 = key2Arr[1];
		
		return sid1.compareTo(sid2);
	}
}



	public static class MySecondarySortReducer extends Reducer<Text, Text, Text, Text> {
		
		Text outVal = new Text();
		// Grouping Comparator determines, we get all the years data per stationID in ascending order. So, stationID wouldn't appear from next Iteration.
		// Aggregates results per stationID using a local HashMap
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		LinkedHashMap<Text, List<Double>> hm = new LinkedHashMap<Text, List<Double>>(); // Local HashMap with key as Year and Value as combination of sumTMIN, minCount, 
																						// sumTMAX and maxCount which takes care of the aggregation
		for(Text value : values) {
			String line = value.toString();
			String[] splitArr = line.split(",");
			String type = new String(splitArr[0]);
			Text year = new Text(splitArr[2]);
		if (type.equals("TMAX")) {
			
			Double temperature = new Double(Double.parseDouble((splitArr[1])));
			if (!hm.containsKey(year)) {
				List<Double> lst = new ArrayList<Double>();
				double maxCount = 1.0;
				lst.add(0.0);
				lst.add(0.0);
				lst.add(temperature);
				lst.add(maxCount);
				hm.put(year, lst);
			}
			else {
				List<Double> lst = new ArrayList<Double>();
				lst = hm.get(year);
				double maxCount = lst.get(3);
				double currMaxTemp = lst.get(2);
				double minCount = lst.get(1);
				double minTemp = lst.get(0);
				lst.clear();
				lst.add(minTemp);
				lst.add(minCount);
				lst.add(currMaxTemp + temperature);
				lst.add(maxCount + 1.0);
				hm.put(year, lst);
			}
		}
		if (type.equals("TMIN")) {
			Double temperature = new Double(Double.parseDouble((splitArr[1])));
			if (!hm.containsKey(year)) {
				List<Double> lst = new ArrayList<Double>();
				double minCount = 1.0;
				lst.add(temperature);
				lst.add(minCount);
				lst.add(0.0);
				lst.add(0.0);
				hm.put(year, lst);
			}
			else {
				List<Double> lst = new ArrayList<Double>();
				lst = hm.get(year);
				double minCount = lst.get(1);
				double currMinTemp = lst.get(0);
				double maxCount = lst.get(3);
				double maxTemp = lst.get(2);
				lst.clear();
				lst.add(currMinTemp + temperature);
				lst.add(minCount + 1.0);
				lst.add(maxTemp);
				lst.add(maxCount);
				hm.put(year, lst);
			}
		}
		}
		
		double meanMinTemp = 0.0;
		double meanMaxTemp = 0.0;
		StringBuilder finalOutput = new StringBuilder();
		finalOutput.append(" - [");
		
		// Calculates meanMaxTemp and meanMinTemp per year per stationID
		for(Text year : hm.keySet()) {
			List<Double> temperatures = new ArrayList<Double>();
			temperatures = hm.get(year);
			
			meanMinTemp = Math.round((temperatures.get(0)/temperatures.get(1))*100.0)/100.0;
			meanMaxTemp = Math.round((temperatures.get(2)/temperatures.get(3))*100.0)/100.0;
			
			finalOutput.append("(" + year + ", " + meanMinTemp + ", " + meanMaxTemp  + "), ");
		}
		finalOutput.append("]");
		
		outVal.set(finalOutput.toString());
		String[] keyArr = key.toString().split(",");
		
		Text stationID= new Text(keyArr[1]);	
		context.write(stationID, outVal);
		
	}
}

	
	//Driver Method
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		BasicConfigurator.configure();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Input/Output Location argument missing");
	      System.exit(2);
	    }
		Job job = Job.getInstance(conf, "SecondarySort");
		job.setJarByClass(SecondarySort.class);
		job.setMapperClass(MySecondarySortMapper.class);
		job.setPartitionerClass(MySecondarySortPartitioner.class);
		job.setSortComparatorClass(MyKeySortComparator.class);
		job.setGroupingComparatorClass(MySecondarySortGroupingComparator.class);
		job.setReducerClass(MySecondarySortReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
		      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		    }
		FileOutputFormat.setOutputPath(job,
			      new Path(otherArgs[otherArgs.length - 1]));
	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
