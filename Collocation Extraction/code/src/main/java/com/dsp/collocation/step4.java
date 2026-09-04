package com.dsp.collocation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.dsp.collocation.util.CombinerUsage;

public class step4 {

    private static final int K = 100;

    public static class Rec {
        public String w1;
        public String w2;
        public double llr;
        public long c12;
        public long c1;
        public long c2;

        public Rec(String w1, String w2, double llr, long c12, long c1, long c2) {
            this.w1 = w1;
            this.w2 = w2;
            this.llr = llr;
            this.c12 = c12;
            this.c1 = c1;
            this.c2 = c2;
        }

        public String toFullValue() {
            return w1 + "\t" + w2 + "\t" + llr + "\t" + c12 + "\t" + c1 + "\t" + c2;
        }
    }

    // Input line from Step3 (TextOutputFormat):
    // decade \t w1 \t w2 \t llr \t c12 \t c1 \t c2
    public static class TopMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\t");
            if (parts.length < 4) return;

            String decade = parts[0];
            String w1 = parts[1];
            String w2 = parts[2];

            double llr;
            try {
                llr = Double.parseDouble(parts[3]);
                if (Double.isNaN(llr) || Double.isInfinite(llr)) return;
            } catch (Exception e) {
                return;
            }

            long c12 = (parts.length > 4) ? safeParseLong(parts[4]) : -1;
            long c1  = (parts.length > 5) ? safeParseLong(parts[5]) : -1;
            long c2  = (parts.length > 6) ? safeParseLong(parts[6]) : -1;

            outKey.set(decade);
            outVal.set(w1 + "\t" + w2 + "\t" + llr + "\t" + c12 + "\t" + c1 + "\t" + c2);
            context.write(outKey, outVal);
        }
    }

    public static class DecadePartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
            if (numReduceTasks == 0) return 0;
            return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    private static ArrayList<Rec> topK(Iterable<Text> values) {
        PriorityQueue<Rec> pq = new PriorityQueue<>(K, Comparator.comparingDouble(r -> r.llr));

        for (Text t : values) {
            Rec r = parseRec(t);
            if (r == null) continue;

            if (pq.size() < K) {
                pq.add(r);
            } else if (r.llr > pq.peek().llr) {
                pq.poll();
                pq.add(r);
            }
        }

        ArrayList<Rec> out = new ArrayList<>(pq);

        // deterministic order
        Collections.sort(out, (a, b) -> {
            int cmp = Double.compare(b.llr, a.llr);
            if (cmp != 0) return cmp;
            cmp = a.w1.compareTo(b.w1);
            if (cmp != 0) return cmp;
            return a.w2.compareTo(b.w2);
        });

        return out;
    }

    private static Rec parseRec(Text v) {
        String[] p = v.toString().split("\t");
        if (p.length < 3) return null;

        String w1 = p[0];
        String w2 = p[1];

        double llr;
        try {
            llr = Double.parseDouble(p[2]);
            if (Double.isNaN(llr) || Double.isInfinite(llr)) return null;
        } catch (Exception e) {
            return null;
        }

        long c12 = (p.length > 3) ? safeParseLong(p[3]) : -1;
        long c1  = (p.length > 4) ? safeParseLong(p[4]) : -1;
        long c2  = (p.length > 5) ? safeParseLong(p[5]) : -1;

        return new Rec(w1, w2, llr, c12, c1, c2);
    }

    private static long safeParseLong(String s) {
        try { return Long.parseLong(s); }
        catch (Exception e) { return -1; }
    }

    // ✅ mergeable combiner: outputs topK per decade in SAME schema
    public static class TopKCombiner extends Reducer<Text, Text, Text, Text> {
        private final Text outVal = new Text();

        @Override
        protected void reduce(Text decade, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            ArrayList<Rec> best = topK(values);
            for (Rec r : best) {
                outVal.set(r.toFullValue());
                context.write(decade, outVal);
            }
        }
    }

    
    public static class TopReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text decade, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            ArrayList<Rec> best = topK(values);
            for (Rec r : best) {
                context.write(decade, new Text(r.w1 + "\t" + r.w2 + "\t" + r.llr));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: step4 <input_path_from_step3> <output_path> [WITH|WITHOUT]");
            System.exit(1);
        }

        CombinerUsage cu = (args.length == 3)
                ? CombinerUsage.fromString(args[2])
                : CombinerUsage.WITH;

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step4 - Top100 (" + cu + ")");

        job.setJarByClass(step4.class);

        job.setMapperClass(TopMapper.class);

        
        if (cu.enabled()) {
            job.setCombinerClass(TopKCombiner.class);
        }

        job.setReducerClass(TopReducer.class);

        job.setPartitionerClass(DecadePartitioner.class);
        job.setNumReduceTasks(4);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
