import java.io.IOException;
//import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ChildName6 {
	public static class NameMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private IntWritable count = new IntWritable();
		private Text childName = new Text();
	      
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	String [] seq =  value.toString().split(",");
			if(seq[3].equals("F") && seq[2].equals("1918")){
				childName.set(seq[1]);
				count.set(Integer.parseInt(seq[4]));
				context.write(childName, count);
			}	
	    }
	}
	
	public static class NameReducer   extends Reducer<Text,IntWritable,Text, IntWritable> {
		private Text KEY = new Text("");
		//private IntWritable result = new IntWritable();
		private IntWritable MAX = new IntWritable(0);
		int limit = 0;
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int sum =0;
			for(IntWritable val: values){
				sum += val.get();
			}
			
			if(sum >= MAX.get()){
				MAX.set(sum);
				KEY.set(key);
			}
			
			//limit = key.getLength(); 
			//while(limit >0){
			//	int sum =0;
			//	for(IntWritable val: values){
			//		sum += val.get();
			//	}
			//	
			//	if(sum >= MAX.get()){
			//		MAX.set(sum);
			//		KEY.set(key);
			//	}
			//	limit -= 1;
			//}
			if(!context.nextKey()) {
				context.write(KEY, MAX); 
			} 			
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    if (args.length != 2) {
	      System.err.println("Usage: child name <in> <out>");
	      System.exit(2);
	    }
	    Job job = Job.getInstance(conf, "child name");
	    job.setJarByClass(ChildName6.class);
	    job.setMapperClass(NameMapper.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(NameReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

}