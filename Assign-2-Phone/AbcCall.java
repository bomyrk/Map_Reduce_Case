import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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

public class ABCcall {
	public static class ABCMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private IntWritable durationinMinutes = new IntWritable();
		private Text phoneNumber = new Text();
	      
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	String [] seq = value.toString().split(",");
			if (seq[4].equals("1")){
				phoneNumber.set(seq[0]);
				String callStartTime = seq[2];
				String callEndTime = seq[3];
				long duration = toDate(callEndTime) - toDate(callStartTime);
				durationinMinutes.set((int)(duration/(1000 * 60))); // we divide by 1000 millisecond times 60 second to get in minutes
				context.write(phoneNumber, durationinMinutes);
			}
			
	    }
		
		private long toDate(String date){ // this is a private function zhich return ti;e in long (millisecond)
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //create new format 
	        Date dateFrm = null;
			try {
				dateFrm = format.parse(date);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} //parse string received in date
			
			return dateFrm.getTime();
			// it will return tim in millisecond
		}
	}
	
	public static class ABCReducer   extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
				}
			if (sum >= 60) {
	        	result.set(sum);		      
		        context.write(key, result);
	        }
			
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    if (args.length != 2) {
	      System.err.println("Usage: ABCcall <in> <out>");
	      System.exit(2);
	    }
	    Job job = Job.getInstance(conf, "ABC call time");
	    job.setJarByClass(ABCcall.class);
	    job.setMapperClass(ABCMapper.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(ABCReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

}
