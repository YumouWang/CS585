package project1;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Query1 {
    
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    	private Text one = new Text();
    	private Text word = new Text();
	
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		    String line = value.toString();
		    StringTokenizer tokenizer = new StringTokenizer(line); 
		    while (tokenizer.hasMoreTokens()) {
				String[] values = tokenizer.nextToken().split(",");
				if (Integer.parseInt(values[3]) > 1 && Integer.parseInt(values[3]) < 7) {
					word.set(values[0]);
					one = new Text(values[1] + "," + values[2] + "," + values[3] + "," + values[4]);
					output.collect(word, one);
				}
		    }
		}
    }

    public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Query1.class);
		conf.setJobName("query1");
	
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
	
		conf.setMapperClass(Map.class);
	
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
	
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
		JobClient.runJob(conf);
    }
}
