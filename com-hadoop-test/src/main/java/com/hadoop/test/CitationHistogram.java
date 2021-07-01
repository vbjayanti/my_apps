package com.hadoop.test;
import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CitationHistogram {
    public static class MapClass
        extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable uno = new IntWritable(1);
        private IntWritable citationCount = new IntWritable();
        public void map(LongWritable key, Text value,
                        Context context) throws IOException,InterruptedException {
            String[] parts = value.toString().split("\\s+");
//            citationCount.set(Integer.parseInt(value.toString()));
            context.write(new Text(parts[1]), uno);
        }
}
public static class Reduce
        extends Reducer<Text, IntWritable, Text, IntWritable>
{
 @Override
 public void reduce(Text key, Iterable<IntWritable> values,
                   Context context) throws IOException,InterruptedException {
    int count = 0;
    for (IntWritable value: values) {
        count += value.get();
    }
    context.write(key, new IntWritable(count));
}
}
public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    Configuration conf = new Configuration();
    conf.set("key.value.separator.in.input.line", ",");
     Job job = Job.getInstance(conf, "customformat");    
    // Job job = new Job(); 
    job.setJarByClass(CitationHistogram.class); 
    job.setJobName("Max temperature");
    FileInputFormat.addInputPath(job, new Path(args[0])); 
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(MapClass.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
