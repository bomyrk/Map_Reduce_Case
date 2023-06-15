import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SpeedCarb {
	public static class SpeedMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private IntWritable one = new IntWritable(1);
		private IntWritable zero = new IntWritable(0);
		private Text perCent = new Text("Percentage");
	      
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	String [] seq =  value.toString().split(",");
			if(Integer.parseInt(seq[1]) >= 65){

				context.write(perCent, one);
				
			} else{
				
				context.write(perCent, zero);
			}
	    }
	}
	
	public static class SpeedReducer   extends Reducer<Text,IntWritable,Text, FloatWritable> {
		private FloatWritable result = new FloatWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			float sum =0;
			float vehFast =0;
			for(IntWritable val : values){
				
				sum += 1;
				
				if(val.get() > 0){
					vehFast += 1;
				}
				
			}
			result.set((vehFast/sum)*100);
			context.write(key, result);
			
			
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    if (args.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
	    Job job = Job.getInstance(conf, "Speed car percent.");
	    job.setJarByClass(SpeedCarb.class);
	    job.setMapperClass(SpeedMapper.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(SpeedReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

}