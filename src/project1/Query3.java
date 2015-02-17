package project1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Query3 {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();

			String line = value.toString();
			if (line == null || line.equals(""))
				return;
			
			if (filePath.contains("customers.txt")) {
				String[] values = line.split(",");

				String id = values[0]; // id
				String name = values[1]; // name
				String salary = values[4]; // salary
				
				context.write(new Text(id), new Text("a#" + name + "," + salary));
			}

			else if (filePath.contains("transactions.txt")) {
				String[] values = line.split(",");

				String id = values[1]; // id
				String transTotal = values[2]; // transTotal
				String transNumItems = values[3]; // transNumberItems

				context.write(new Text(id), new Text("b#" + transTotal + "," + transNumItems));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			List<String> listA = new ArrayList<String>();
			List<String> listB = new ArrayList<String>();

			for (Text value : values) {

				String val = value.toString();
				int indexA = val.lastIndexOf("a#");
				int indexB = val.lastIndexOf("b#");

				if (indexB == -1) {
					listA.add(val.substring(indexA + 2));
				} else if (indexA == -1) {
					listB.add(val.substring(indexB + 2));
				}
			}

			int sizeA = listA.size();
			int sizeB = listB.size();
			int i, j;
			float totalSum = 0;
			int minItems = 10;
			for (i = 0; i < sizeA; i++) {
				for (j = 0; j < sizeB; j++) {
					String[] numbers = listB.get(j).toString().split(",");
					float transTotal = Float.parseFloat(numbers[0]);
					int numOfItems = Integer.parseInt(numbers[1]);
					totalSum += transTotal;
					if (numOfItems < minItems) {
						minItems = numOfItems;
					}
				}
				context.write(key, new Text(listA.get(i) + "," + String.valueOf(sizeB) + "," + String.valueOf(totalSum) + ","
								+ String.valueOf(minItems)));
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Query3");
		job.setJobName("Query3");

		job.setJarByClass(Query3.class);
		job.setMapperClass(MapClass.class);
		// job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

