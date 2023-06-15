import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;

public class ChildName7 {

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		//String[] otherArgs 	= new GenericOptionsParser(conf, args).getRemainingArgs();

		// if less than two paths
		// provided will show error
		if (args.length < 2) {
			System.err.println(" Error: please provide two paths");
			System.exit(2);
		}

		Job job
			= Job.getInstance(conf, " top 1 Male 1945 ");
		job.setJarByClass(ChildName7.class);

		job.setMapperClass(Top_1m_Mapper.class);
		job.setReducerClass(Top_1m_Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(
			job, new Path(args[0]));
		FileOutputFormat.setOutputPath(
			job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
