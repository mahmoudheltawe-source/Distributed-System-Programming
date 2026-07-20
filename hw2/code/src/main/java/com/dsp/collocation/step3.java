package com.dsp.collocation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.dsp.collocation.util.util_class;

public class step3 {

    /**
     * We use a composite Text key: "decade\t0" for UNI, "decade\t1" for BI
     * Sorting puts UNI first, but grouping groups by decade only.
     * => reducer sees all UNI first, builds unigram map and N, then streams BI and computes LLR.
     */
    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (line == null || line.isEmpty()) return;

            String[] parts = line.split("\t");

            // Step1 output: decade \t w1 \t w2 \t c12
            if (parts.length == 4) {
                String decade = parts[0];
                String w1 = parts[1];
                String w2 = parts[2];
                String c12 = parts[3];

                outKey.set(decade + "\t1"); // BI after UNI
                outVal.set("BI\t" + w1 + "\t" + w2 + "\t" + c12);
                context.write(outKey, outVal);
                return;
            }

            // Step2 output: decade \t word \t count
            if (parts.length == 3) {
                String decade = parts[0];
                String word = parts[1];
                String count = parts[2];

                outKey.set(decade + "\t0"); // UNI first
                outVal.set("UNI\t" + word + "\t" + count);
                context.write(outKey, outVal);
            }
        }
    }

    /** Partition only by decade (ignore the 0/1 tag) */
    public static class DecadePartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            if (numPartitions == 0) return 0;
            String s = key.toString();
            int tab = s.indexOf('\t');
            String decade = (tab >= 0) ? s.substring(0, tab) : s;
            return (decade.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    /** Group by decade only (ignore the 0/1 tag) */
    public static class DecadeGroupingComparator extends WritableComparator {
        protected DecadeGroupingComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String s1 = a.toString();
            String s2 = b.toString();
            int t1 = s1.indexOf('\t');
            int t2 = s2.indexOf('\t');
            String d1 = (t1 >= 0) ? s1.substring(0, t1) : s1;
            String d2 = (t2 >= 0) ? s2.substring(0, t2) : s2;
            return d1.compareTo(d2);
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        // word -> c(word) for this decade
        private final Map<String, Long> unigram = new HashMap<>(1 << 16);
        private long N = 0;

        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void reduce(Text groupedKey, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // groupedKey looks like "1990s\t0" (first in group), extract decade
            String k = groupedKey.toString();
            String decade = k.split("\t")[0];

            unigram.clear();
            N = 0;

            // IMPORTANT: values arrive in sorted key order:
            // first all "decade\t0" (UNI), then all "decade\t1" (BI)
            for (Text t : values) {
                String[] p = t.toString().split("\t");
                if (p.length == 0) continue;

                if ("UNI".equals(p[0])) {
                    if (p.length < 3) continue;
                    String word = p[1];
                    long count;
                    try {
                        count = Long.parseLong(p[2]);
                    } catch (Exception e) {
                        continue;
                    }

                    if ("__TOTAL__".equals(word)) {
                        N += count;
                    } else {
                        // if duplicates appear, sum them
                        unigram.merge(word, count, Long::sum);
                    }
                } else if ("BI".equals(p[0])) {
                    if (p.length < 4) continue;
                    String w1 = p[1];
                    String w2 = p[2];
                    long c12;
                    try {
                        c12 = Long.parseLong(p[3]);
                    } catch (Exception e) {
                        continue;
                    }

                    Long c1 = unigram.get(w1);
                    Long c2 = unigram.get(w2);

                    if (c1 == null || c2 == null) continue;
                    if (c1 <= 0 || c2 <= 0 || c12 <= 0 || N <= 0) continue;

                    double llr = util_class.compute(c12, c1, c2, N);

                    outKey.set(decade + "\t" + w1 + "\t" + w2);
                    outVal.set(llr + "\t" + c12 + "\t" + c1 + "\t" + c2);
                    context.write(outKey, outVal);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: step3 <step1_output> <step2_output> <step3_output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        // extra safety for this job
        conf.setInt("mapreduce.reduce.memory.mb", 8192);
        conf.set("mapreduce.reduce.java.opts", "-Xmx6500m");

        Job job = Job.getInstance(conf, "Step3 Join + LLR (secondary sort)");

        job.setJarByClass(step3.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setPartitionerClass(DecadePartitioner.class);
        job.setGroupingComparatorClass(DecadeGroupingComparator.class);

        // spread decades across reducers (VERY IMPORTANT)
        job.setNumReduceTasks(8);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
