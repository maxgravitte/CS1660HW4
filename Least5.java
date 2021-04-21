import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;



public class Least5 {
	public static class Least5Mapper extends Mapper<Object, Text, Text, LongWritable> {
		private TreeMap<Long, String> treeMap;
		static int n=5;
		//Grab N from the context.
		//Instantiate new Treemap
		@Override
	    public void setup(Context context) throws IOException,InterruptedException
	    {
			Configuration conf = context.getConfiguration();
			
	        treeMap = new TreeMap<Long, String>();
	    }
		
		@Override
		public void map(Object key, Text value, Context context) {
			//Split line by tabs -> token, frequency.
			String[] arr = value.toString().split("\t");
			if(arr.length>1) {
				String name = arr[0];
				long count = Long.parseLong(arr[1]);
				treeMap.put(count, name);
			}
			//Remove smallest
			if (treeMap.size()>n) {
				treeMap.remove(treeMap.lastKey());
			}
		}
		@Override
	    public void cleanup(Context context) throws IOException, InterruptedException
	    {
			//Add the Mapper's final results to the context
	        for (Map.Entry<Long, String> entry : treeMap.entrySet()) 
	        {
	        	long count = entry.getKey();
	            String name = entry.getValue();
	            context.write(new Text(name), new LongWritable(count));
	        }
	    }
	}
	public static class Least5Reducer extends Reducer<Text, LongWritable, LongWritable, Text> {
		private TreeMap<Long, String> treeMap2;
		static int n=5;
		@Override
	    public void setup(Context context) throws IOException, InterruptedException
	    {
			treeMap2 = new TreeMap<Long, String>();
			Configuration conf = context.getConfiguration();
	    }
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			//Repeat of Mapper but we are combining the results together.
			String name = key.toString();
			long count = 0;
			for (LongWritable val : values) {
				count = val.get();
			}
			treeMap2.put(count, name);
			if(treeMap2.size()>n) {
				treeMap2.remove(treeMap2.lastKey());
			}
		}
		@Override
	    public void cleanup(Context context) throws IOException, InterruptedException
	    {
	        for (Map.Entry<Long, String> entry : treeMap2.entrySet()) 
	        {
	        	//add final results to context
	            long count = entry.getKey();
	            String name = entry.getValue();
	            context.write(new LongWritable(count), new Text(name));
	        }
	    }

	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
  
        // if less than two paths 
        // provided will show error
        if (otherArgs.length !=2) 
        {
            System.err.println("Error: please provide two paths");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "least 5");
        job.setJarByClass(Least5.class);
  
        job.setMapperClass(Least5Mapper.class);
        job.setReducerClass(Least5Reducer.class);
  
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
  
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.waitForCompletion(true);
	}
}
