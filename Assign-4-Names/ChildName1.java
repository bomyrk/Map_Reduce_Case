import java.io.IOException;


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


public class ChildName {
	public static class NameMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private IntWritable one = new IntWritable(1);
		private Text childName = new Text();
	      
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	String [] seq =  value.toString().split(",");
			if(seq[1].startsWith("G")){
				childName.set(seq[1]);
				context.write(childName, one);
				
			}
	    }
	}
	
	public static class NameReducer   extends Reducer<Text,IntWritable,Text, Text> {
		private Text result = new Text("");
		//private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			//int sum =0;
			//for(IntWritable val: values){
			//	sum += val.get();
			//}
			//result.set(sum);
			context.write(key, result);
			
			
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    if (args.length != 2) {
	      System.err.println("Usage: child name <in> <out>");
	      System.exit(2);
	    }
	    Job job = Job.getInstance(conf, "child name");
	    job.setJarByClass(ChildName.class);
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
