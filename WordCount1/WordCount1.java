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

public class WordCount1 {

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

		String[] search = new String[] {"education", "politics", "sports", "agriculture"};
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// Store split value of the string.
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

			ArrayList<String> results = new ArrayList<String>();
			int maxCount = 0;

			ArrayList<String> str = new ArrayList<String>();

			for (Text val : values) {
				str.add(val.toString());
			}

			int index = 0;
			int count = -1;
			for(int i = 0; i < str.size(); i++) {
				if( Integer.parseInt( str.get(i).split("-")[1] ) > count ) {
					index = i;
					count = Integer.parseInt( str.get(i).split("-")[1] );
				}
			}

			context.write( new Text(str.get(index)), new Text(str.get(index)) );
		}
	}


	public static class CondenseMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String phrase = value.toString().trim().split("-")[0].trim();
			context.write(new Text(phrase), new IntWritable(1));

		}
	}

	public static class CondenseReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));

		}
	}

	// Main function.
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		//Job Number 1
		Job job = Job.getInstance(conf, "Total Compute ");
		job.setJarByClass(WordCount1.class);
		job.setMapperClass(FullDocumentMapper.class);
		job.setCombinerClass(FullDocumentReducer.class);
		job.setReducerClass(FullDocumentReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Set input and output paths.

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path( "/user/total_compute1" /*args[1]*/));

		// Exit when done.
		job.waitForCompletion(true);

		//Job Number 2
		Job job2 = Job.getInstance(conf, "Manipulation");
		job2.setJarByClass(WordCount1.class);
		job2.setMapperClass(ManipulationMapper.class);
		job2.setCombinerClass(ManipulationReducer.class);
		job2.setReducerClass(ManipulationReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		// Set input and output paths.
		FileInputFormat.addInputPath(job2, new Path("/user/total_compute1" /*args[0]*/));
		FileOutputFormat.setOutputPath(job2, new Path("/user/manipulation1" /*args[1]*/));

		// Exit when done.
		job2.waitForCompletion(true);

		//Job Number 3
		Job job3 = Job.getInstance(conf, "Condenser Job");
		job3.setJarByClass(WordCount1.class);
		job3.setMapperClass(CondenseMapper.class);
		job3.setCombinerClass(CondenseReducer.class);
		job3.setReducerClass(CondenseReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);

		// Set input and output paths.
		FileInputFormat.addInputPath(job3, new Path("/user/manipulation1" /*args[0]*/));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));

		// Exit when done.
		System.exit(job3.waitForCompletion(true) ? 0 : 1);

	}

}