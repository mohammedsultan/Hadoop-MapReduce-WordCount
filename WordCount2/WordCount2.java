/**
Name: Mohammed Sultan Masihuddin
USID: msm76
 */

// Imports
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import java.util.regex.Pattern;
import java.util.*;

public class WordCount2 {

	// Mapper Class
	public static class FullDocumentMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		// These are the key words to search for.
		String[] search = new String[] {"education", "politics", "sports", "agriculture"};
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			String filepath = ((FileSplit) context.getInputSplit()).getPath().toString();
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken().toLowerCase());
				for(int i = 0; i < search.length; i++) {
					while(word.toString().contains(search[i])) {
						context.write(new Text(filepath + "/" + search[i]), one);
						word.set(word.toString().replaceFirst(search[i], ""));
					}
				}
			}
		}
	}
	public static class FullDocumentReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			String tmp = key.toString().toLowerCase();

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class ManipulationMapper extends Mapper<Object, Text, Text, Text>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		// These are the key words to search for.
		String[] search = new String[] {"education", "politics", "sports", "agriculture"};

		// Map function
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = new String[] {"", ""};
			int counter = 0;
			StringTokenizer st = new StringTokenizer(value.toString());
			while (st.hasMoreTokens()) {

				values[counter] = st.nextToken();
				counter += 1;

			}
			counter = 0;
			int lastElemIndex = values[0].lastIndexOf("/");
			String state = values[0].substring(0, lastElemIndex);
			String phrase = values[0].substring(lastElemIndex + 1, values[0].length());
			StringBuilder sb = new StringBuilder();
			sb.append(phrase).append("-").append(values[1]);

			context.write(new Text(state), new Text(sb.toString()));

		}
	}
	public static class ManipulationReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			for (Text val : values) {
				sb.append(val.toString()).append(",");
			}

			context.write( key, new Text( sb.toString() ) );

		}
	}

	public static class SortMapper extends Mapper<Object, Text, Text, IntWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] breakdown = value.toString().split("\t");
			String list = breakdown[1];
			String[] rank = list.split(",");
			Map<Integer, String> map = new HashMap<Integer, String>();
			for(int i = 0; i < rank.length; i++) {
				String[] current = rank[i].split("-");
				map.put( Integer.parseInt( current[1] ) , current[0] );
			}
			StringBuilder result = new StringBuilder();
			Map<Integer, String> treeMap = new TreeMap<Integer, String>(map);
			for(Integer val : treeMap.keySet()) {
				result.append( map.get( val ) ).append(",");
				map.remove( val );
			}

			context.write( new Text( result.toString() ), new IntWritable(1));

		}
	}
	public static class SortReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
			}

			context.write( key , new IntWritable(sum));

		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		//Job Number 1
		Job job = Job.getInstance(conf, "Total Compute");
		job.setJarByClass(WordCount2.class);
		job.setMapperClass(FullDocumentMapper.class);
		job.setCombinerClass(FullDocumentReducer.class);
		job.setReducerClass(FullDocumentReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Set input and output paths.

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path( "/user/total_compute2" ));

		// Exit when done.
		job.waitForCompletion(true);

		//Job Number 2
		// Set up a new job and give it a name.
		Job job2 = Job.getInstance(conf, "Manipulation");
		job2.setJarByClass(WordCount2.class);
		job2.setMapperClass(ManipulationMapper.class);
		job2.setCombinerClass(ManipulationReducer.class);
		job2.setReducerClass(ManipulationReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		// Set input and output paths.
		FileInputFormat.addInputPath(job2, new Path( "/user/total_compute2"));
		FileOutputFormat.setOutputPath(job2, new Path("/user/manipulation2"));
		job2.waitForCompletion(true);

		//Job Number 3
		// Set up a new job and give it a name.
		Job job3 = Job.getInstance(conf, "Sort Job");
		job3.setJarByClass(WordCount2.class);
		job3.setMapperClass(SortMapper.class);
		job3.setCombinerClass(SortReducer.class);
		job3.setReducerClass(SortReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);

		// Set input and output paths.
		FileInputFormat.addInputPath(job3, new Path("/user/manipulation2"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));

		// Exit when done.
		System.exit(job3.waitForCompletion(true) ? 0 : 1);

	}

}