package hw2.cs6240;

import java.io.IOException;

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

public class NoCombiner {
		  
	public static class MyTemperatureMapper 
		       extends Mapper<Object, Text, Text, Text>{
		
		// Mapper class writes the key and Text to the Context	
		// Key is stationID and value is Text which is a combination of type and temperature	     
		    public void map(Object key, Text value, Context context
		                    ) throws IOException, InterruptedException {
		    	Text outVal;
				String line = value.toString();
				String[] splitArr = line.split(",");
				if(splitArr[2].equals("TMAX") || splitArr[2].equals("TMIN")){
						Text stationID = new Text(splitArr[0]);
						Text type =  new Text(splitArr[2]);
						Text temperature = new Text((splitArr[3]));
						outVal = new Text(type+","+temperature);
						context.write(stationID, outVal);
		      }
		    }
		  }
	public static class MyTemperatureReducer
				extends Reducer<Text, Text, Text, Text> {
		
		// Reducer class receives key(stationID) and List of Text(type and temperature) 
		// The Reducer aggregates the temperature values i.e. Calculates sum of TMAX, TMAXCount and sum of TMIN, TMINCount for a particular station and then finds the Mean
		// writes it to a Text separated by comma.
		Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Double sumMaxTemp = 0.0;
			Double sumMinTemp = 0.0;
			Double minCount = 0.0;
			Double maxCount = 0.0;
			
			for(Text value : values) {
				String val = value.toString();
				String[] valArr = val.split(",");
				if(valArr[0].equals("TMAX")) {
					Double maxTemperature = Double.parseDouble(valArr[1]);
					sumMaxTemp+= maxTemperature;
					maxCount++;
				}else {
					Double minTemperature = Double.parseDouble(valArr[1]);
					sumMinTemp+= minTemperature;
					minCount++;
				}
			}
			Double meanMaxTemp = sumMaxTemp/maxCount;
			Double meanMinTemp = sumMinTemp/minCount;
			
			result.set(new String(", " + meanMinTemp + ", " + meanMaxTemp));
			context.write(key, result);
		}
	}
	
	//  Driver Method
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		BasicConfigurator.configure();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Input/Output Location argument missing");
	      System.exit(2);
	    }
	    Job job = Job.getInstance(conf, "NoCombiner");
	    job.setJarByClass(NoCombiner.class);
	    job.setMapperClass(MyTemperatureMapper.class);
	    job.setReducerClass(MyTemperatureReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
