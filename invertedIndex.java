import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class invertedIndex 
{
	public static class Map extends Mapper<LongWritable, Text, wordPair, Text> 
	{
		private wordPair pair = new wordPair();
		private HashMap<wordPair, Text> hash = new HashMap<>();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			String line = value.toString();
			String words[] = line.split(" ");

			pair.setNeighbor(fileName);
			for (String s : words) 
			{
				pair.setWord(s);

				String firstWord = pair.getWord().toString();
				String secondWord = pair.getNeighbor().toString();
				wordPair pair2 = new wordPair(firstWord, secondWord);
				
				if(hash.containsKey(pair2)) 
				{
					int sum = Integer.valueOf(hash.get(pair2).toString()) + 1;
					Text freq = new Text(String.valueOf(sum));
					hash.replace(pair2, freq);
				}
				else 
				{
					hash.put(pair2, new Text("1"));
				}
			}
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException 
		{
			for(HashMap.Entry<wordPair, Text> entry : hash.entrySet()) 
			{
				context.write(entry.getKey(), entry.getValue());
			}
		}
	}

	public static class Reduce extends Reducer<wordPair, Text, Text, Text> 
	{
		private String current = "";
		private String pos = "";
		boolean start = true;

		@Override
		protected void reduce(wordPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			String first = key.getWord().toString();
			String second = key.getNeighbor().toString();
			int freq = getCount(values);

			if (first.equals(current)) 
			{
				pos = pos + ';' + second + ':' + freq;
			}
			else 
			{
				if (!start) 
				{
					context.write(new Text(current), new Text(pos));
				}
				current = first;
				pos = second + ":" + freq;
				start = false;
			}
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException 
		{
			context.write(new Text(current), new Text(pos));
		}

		public int getCount(Iterable<Text> values) 
		{
			int count = 0;
			for (Text value : values) 
			{
				count += Integer.valueOf(value.toString());
			}
			return count;
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "inverted index");
		job.setNumReduceTasks(3);
		job.setJarByClass(invertedIndex.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(wordPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}