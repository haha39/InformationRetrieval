import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class hw5 {

    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, ",");
            // while (tokenizer.hasMoreTokens()) {
            String token1 = tokenizer.nextToken();
            String token2 = tokenizer.nextToken();
            String token3 = tokenizer.nextToken();
            float avg = 0;

            if (token3.equals("PM2.5")) {
                float total = 0;
                int div = 0;

                while (tokenizer.hasMoreTokens()) {
                    String token_tmp = tokenizer.nextToken();

                    try {
                        total += Float.parseFloat(token_tmp);
                        div++;
                    } catch (Exception e) {

                    }
                }

                avg = total / div;
                context.write(new Text(token1 + "," + token2 + "," + token3), new FloatWritable(avg));
            }
            // }
        }
    }

    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0;

            for (FloatWritable val : values) {
                sum += val.get();
            }

            context.write(key, new FloatWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "wordcount2.0");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setJarByClass(WordCount.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}