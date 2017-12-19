package hw2.cs6240;

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


public class InMapperComb {
	// Mapper locally aggregates the sum and count per stationID
	public static class MyTemperatureMapper extends Mapper<Object, Text, Text, Text> {
		HashMap<Text, List<Double>> hm; // HashMap to locally keep sumMinTemp, MinCount, sumMaxTemp and MaxCount per stationID
		Text outVal = new Text();
		
		protected void setup(Context context)
				throws IOException, InterruptedException {
				hm =  new HashMap<Text, List<Double>>();
				}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] splitArr = line.split(",");
			String type = new String(splitArr[2]);
			if (type.equals("TMAX")) {
				Text stationID = new Text(splitArr[0]);
				Double temperature = new Double(Double.parseDouble((splitArr[3])));
				if (!hm.containsKey(stationID)) {	// Updating maxCount and temperature
					List<Double> lst = new ArrayList<Double>();
					double maxCount = 1.0;
					lst.add(0.0);
					lst.add(0.0);
					lst.add(temperature);
					lst.add(maxCount);
					hm.put(stationID, lst);
				}
				else {
					List<Double> lst = new ArrayList<Double>();
					lst = hm.get(stationID);
					double maxCount = lst.get(3);
					double currMaxTemp = lst.get(2);
					double minCount = lst.get(1);
					double minTemp = lst.get(0);
					lst.clear();
					lst.add(minTemp);
					lst.add(minCount);
					lst.add(currMaxTemp + temperature);
					lst.add(maxCount + 1.0);
					hm.put(stationID, lst);
				}
			}
			if (type.equals("TMIN")) {
				Text stationID = new Text(splitArr[0]);
				Double temperature = new Double(Double.parseDouble((splitArr[3])));
				if (!hm.containsKey(stationID)) { //Updating MinCount and temperature
					List<Double> lst = new ArrayList<Double>();
					double minCount = 1.0;
					lst.add(temperature);
					lst.add(minCount);
					lst.add(0.0);
					lst.add(0.0);
					hm.put(stationID, lst);
				}
				else {
					List<Double> lst = new ArrayList<Double>();
					lst = hm.get(stationID);
					double minCount = lst.get(1);
					double currMinTemp = lst.get(0);
					double maxCount = lst.get(3);
					double maxTemp = lst.get(2);
					lst.clear();
					lst.add(currMinTemp + temperature);
					lst.add(minCount + 1.0);
					lst.add(maxTemp);
					lst.add(maxCount);
					hm.put(stationID, lst);
				}
			}
			
				
			}
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			for(Text key : hm.keySet()) { //Extracts the local aggregates from the HashMap separately and writes it in form of Text
				
				List<Double> lst = hm.get(key);
				outVal.set("TMIN" + "," + lst.get(0) + "," + lst.get(1));
				context.write(key, outVal);
				
				outVal.set("TMAX" + "," + lst.get(2) + "," + lst.get(3));
				context.write(key, outVal);
				
			}
}
		}
	

		public static class MyTemperatureReducer extends Reducer<Text, Text, Text, Text> {
		Text result = new Text();
		// Reducer takes multiple local sumMinTemp and sumMaxTemp per stationID.
		// Aggregates it and finds the average.
		// Writes it in the form of Text(TMIN average and TMAX average)
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Double sumMaxTemp = 0.0;
			Double sumMinTemp = 0.0;
			Double minCount = 0.0;
			Double maxCount = 0.0;

			for (Text value : values) {
				String val = value.toString();
				String[] valArr = val.split(",");
				if (valArr[0].equals("TMAX")) {
					Double maxTemperature = Double.parseDouble(valArr[1]);
					sumMaxTemp += maxTemperature;
					maxCount += Double.parseDouble(valArr[2]);
				} else {
					Double minTemperature = Double.parseDouble(valArr[1]);
					sumMinTemp += minTemperature;
					minCount += Double.parseDouble(valArr[2]);
				}
			}
			Double meanMaxTemp = sumMaxTemp / maxCount;
			Double meanMinTemp = sumMinTemp / minCount;

			result.set(new String(", " + meanMinTemp + ", " + meanMaxTemp));
			context.write(key, result);
		}
	}

	
	// Driver Method
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		BasicConfigurator.configure();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Input/Output Location argument missing");
	      System.exit(2);
	    }
		Job job = Job.getInstance(conf, "InMapperComb");
		job.setJarByClass(InMapperComb.class);
		job.setMapperClass(MyTemperatureMapper.class);
		job.setReducerClass(MyTemperatureReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
