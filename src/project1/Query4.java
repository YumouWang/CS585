package project1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
 
import java.util.Vector;

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

public class Query4 {
    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
    	private Map<String, String> countryCodeMap = new HashMap<String, String>();
    	
    	@Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader in = null;    
            try {
                Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                String customer = null;
                for (Path path : paths) {
                    if (path.toString().contains("customers.txt")) {
                        in = new BufferedReader(new FileReader(path.toString()));
                        while (null != (customer = in.readLine())) {
                        	String[] tuple = customer.split(",");
                        	countryCodeMap.put(tuple[0], tuple[3]);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    	 
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	 
            String[] line = value.toString().split(",");     
            
            if (countryCodeMap.containsKey(line[1])) {
            	context.write(new Text(countryCodeMap.get(line[1].trim())), new Text(line[1].trim() + "," + line[2].trim()));
            } 
        }
    }
 
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
 
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
 
            float minTransTotal = 1000;
            float maxTransTotal = 10;
            int count = 0;
            List<Integer> idList = new ArrayList<Integer>();
        	for (Text value : values) {
                String[] val = value.toString().split(",");  
                int id = Integer.parseInt(val[0]);
                float transTotal = Float.parseFloat(val[1]);
 		
                if (transTotal < minTransTotal) {
                	minTransTotal = transTotal;
                }
                if (transTotal > maxTransTotal) {
                	maxTransTotal = transTotal;
                }
				if (!idList.contains(id)) {
					idList.add(id);
				}
            }  
            context.write(key, new Text(String.valueOf(idList.size()) + "," + String.valueOf(minTransTotal) + "," + String.valueOf(maxTransTotal)));
        }
    }
    
    public static void main(String[] args) throws Exception {
    	
    	Configuration conf = new Configuration();
    	Job job = new Job(conf, "Query4");
        job.setJobName("Query4");

        job.setJarByClass(Query4.class);
        job.setMapperClass(MapClass.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
  
        job.setInputFormatClass(TextInputFormat.class);
  
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

