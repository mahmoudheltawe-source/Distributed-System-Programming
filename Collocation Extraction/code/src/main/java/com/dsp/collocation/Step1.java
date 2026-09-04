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

public class Step1 {

    public static class BigramMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text outKey;
        private IntWritable outValue;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            outKey = new Text();
            outValue = new IntWritable();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            System.err.println("MAP INPUT RAW: " + value.toString());

            String line = value.toString();
            String[] values = line.split("\t");

            // [ "w1 w2", year, count, pageCount ]
            if (values.length < 4) {
                System.err.println("SKIP: less than 4 fields");
                System.out.println("Invalid record: " + line);
                return;
            }
            String[] words = values[0].split("\\s+");
            if (words.length != 2) {
                System.err.println("SKIP: not a bigram");
                System.out.println("Invalid bigram: " + values[0]);
                return;
            }

            String word1 = words[0];
            String word2 = words[1];
            // String hebrewPattern = "[^\\p{InHebrew}\\p{Nd}]+";
            String validPattern = "[^\\p{InHebrew}a-zA-Z\\p{Nd}]+";

            word1 = word1.replaceAll(validPattern, "").trim();
            word2 = word2.replaceAll(validPattern, "").trim();

            // RE-CHECK: If cleaning resulted in empty words, skip the record.
            if (word1.isEmpty() || word2.isEmpty()) {
                System.err.println("SKIP: Word empty after cleaning: [" + word1 + ", " + word2 + "]");
                return;
            }

            if (word1.matches("\\d+") || word2.matches("\\d+")) {
                return;
            }

            // check if the word is a stopword
            if (util_class.isStopWord(word1) || util_class.isStopWord(word2)) {
                System.err.println("SKIP: stopword: [" + word1 + ", " + word2 + "]");
                System.out.println("Stop word found: " + word1 + " or " + word2);
                return;
            }


            int year;
            int count;

            try {
                year = Integer.parseInt(values[1]);
                count = Integer.parseInt(values[2]);
            } catch (Exception e) {
                System.err.println("SKIP: parse error");

                System.out.println("Invalid year or count in record: " + line);
                return;
            }

            String decade = util_class.getDecade(year);

            outKey.set(decade + "\t" + word1 + "\t" + word2);
            outValue.set(count);
            System.err.println("MAP OUTPUT: " + outKey + " -> " + count);

            context.write(outKey, outValue);
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) { // value is IntWritable
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class BigramReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            StringBuilder debugValues = new StringBuilder();

            for (IntWritable v : values) {
                sum += v.get();
                debugValues.append(v.get()).append(",");
            }
            System.err.println("REDUCE INPUT: " + key + " <- [" + debugValues + "]");
            System.err.println("REDUCE OUTPUT: " + key + " -> " + sum);
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

       if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: Step1 <input path> <output path> [WITH|WITHOUT]");
            System.exit(-1);
        }

        CombinerUsage cu = (args.length == 3)
            ? CombinerUsage.fromString(args[2])
            : CombinerUsage.WITH; // default same as your old behavior

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bigram Count Job (" + cu + ")");

        job.setJarByClass(Step1.class);
        job.setMapperClass(BigramMapper.class);
        job.setReducerClass(BigramReducer.class);

        job.setPartitionerClass(PartitionerClass.class);

    
        if (cu.enabled()) {
            job.setCombinerClass(BigramReducer.class);
        }

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}