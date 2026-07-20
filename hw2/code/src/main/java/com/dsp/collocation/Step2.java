package com.dsp.collocation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.dsp.collocation.util.CombinerUsage;
import com.dsp.collocation.util.util_class;


public class Step2 {

    public static class UnigramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text outKey = new Text();
        private final IntWritable outValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            String[] parts = line.split("\t");
            if (parts.length < 3) {
                return;
            }
            String word = parts[0];
            int year;
            int count;
            try {
                year = Integer.parseInt(parts[1]);
                count = Integer.parseInt(parts[2]);
            } catch (NumberFormatException e) {
                return;
            }

            if (util_class.isStopWord(word)) {
                return;
            }

            String decade = util_class.getDecade(year);
            outKey.set(decade + "\t" + word);
            outValue.set(count);

            context.write(outKey, outValue);
            outKey.set(decade + "\t" + "__TOTAL__");
            outValue.set(count);
            context.write(outKey, outValue);
        }
    }

    public static class DecadePartitioner extends Partitioner<Text, IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {

            // key format = "1990s\tword"
            String full = key.toString();
            String decade = full.split("\t")[0];

            // Partition based on decade hash
            return (decade.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static class UnigramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: Step2 <inputPath> <outputPath> [WITH|WITHOUT]");
            System.exit(1);
        }

        CombinerUsage cu = (args.length == 3)
                ? CombinerUsage.fromString(args[2])
                : CombinerUsage.WITH; // default same as before

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step2 - Unigram Count (" + cu + ")");
        job.setJarByClass(Step2.class);

        job.setMapperClass(UnigramMapper.class);
        job.setReducerClass(UnigramReducer.class);

        if (cu.enabled()) {
            job.setCombinerClass(UnigramReducer.class);
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setPartitionerClass(DecadePartitioner.class);
        job.setNumReduceTasks(4);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}