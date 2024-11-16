import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TextAnalysis {

    // Line Count Mapper
    public static class LineMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static Text lineKey = new Text("Total Lines");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(lineKey, one);
        }
    }

    // Line Count Reducer
    public static class LineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int lineCount = 0;
            for (IntWritable val : values) {
                lineCount += val.get();
            }
            context.write(key, new IntWritable(lineCount));
        }
    }

    // Word Count Mapper
    public static class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    // Word Count Reducer
    public static class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Mapper for Word and Line Totals (for Average Calculation)
    public static class TotalMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static Text lineKey = new Text("Total Lines");
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(lineKey, one); // Counts lines
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                itr.nextToken();
                context.write(new Text("Total Words"), one); // Counts words
            }
        }
    }

    // Reducer to Calculate Average Words Per Line
    public static class AverageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private int lineCount = 0;
        private int wordCount = 0;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (key.toString().equals("Total Lines")) {
                lineCount = sum;
            } else if (key.toString().equals("Total Words")) {
                wordCount = sum;
            }
            context.write(key, new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int averageWordsPerLine = lineCount > 0 ? wordCount / lineCount : 0;
            context.write(new Text("Average Words Per Line"), new IntWritable(averageWordsPerLine));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Line Count Job
        Job lineJob = Job.getInstance(conf, "line count");
        lineJob.setJarByClass(TextAnalysis.class);
        lineJob.setMapperClass(LineMapper.class);
        lineJob.setReducerClass(LineReducer.class);
        lineJob.setOutputKeyClass(Text.class);
        lineJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(lineJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(lineJob, new Path(args[1] + "/lineCount"));
        lineJob.waitForCompletion(true);

        // Word Count Job
        Job wordJob = Job.getInstance(conf, "word count");
        wordJob.setJarByClass(TextAnalysis.class);
        wordJob.setMapperClass(WordMapper.class);
        wordJob.setReducerClass(WordReducer.class);
        wordJob.setOutputKeyClass(Text.class);
        wordJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(wordJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(wordJob, new Path(args[1] + "/wordCount"));
        wordJob.waitForCompletion(true);

        // Average Words Per Line Job
        Job avgJob = Job.getInstance(conf, "average words per line");
        avgJob.setJarByClass(TextAnalysis.class);
        avgJob.setMapperClass(TotalMapper.class);
        avgJob.setReducerClass(AverageReducer.class);
        avgJob.setOutputKeyClass(Text.class);
        avgJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(avgJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(avgJob, new Path(args[1] + "/averageWordsPerLine"));
        System.exit(avgJob.waitForCompletion(true) ? 0 : 1);
    }
}