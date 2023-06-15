import java.io.*;
import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top_1m_Mapper
	extends Mapper<Object, Text, Text, LongWritable> {

	private TreeMap<Long, String> tmap;
	//private Text childName = new Text();

	@Override
	public void setup(Context context)
		throws IOException, InterruptedException
	{
		tmap = new TreeMap<Long, String>();
	}

	@Override
	public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException
	{

		// input data format => movie_name
		// no_of_views (comma separated)
		// we split the input data
		String []  seq =  value.toString().split(",");

		//String name = tokens[0];
		//long no_of_used = Long.parseLong(tokens[1]);
		
		if(seq[3].equals("M") && seq[2].equals("1945")) {
			String childName = seq[1];
			long no_of_used  = Integer.parseInt(seq[4]);
			//context.write(childName, count);
	
		
			// insert data into treeMap,
			// we want top 1 used 
			// so we pass no_of_used as key
			tmap.put(no_of_used, childName);

			// we remove the first key-value
			// if it's size increases 1
			if (tmap.size() > 1) {
				tmap.remove(tmap.firstKey());
			}
		}
	}

	@Override
	public void cleanup(Context context)
		throws IOException, InterruptedException
	{
		for (Map.Entry<Long, String> entry :
			tmap.entrySet()) {

			long count = entry.getKey();
			String name = entry.getValue();

			context.write(new Text(name),
						new LongWritable(count));
		}
	}
}