import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TextAnalysisDriver {
    public static void main(String[] args) throws Exception {
        // Line Count Job
        Job lineCountJob = Job.getInstance(new Configuration(), "Line Count");
        lineCountJob.setJarByClass(TextAnalysisDriver.class);
        lineCountJob.setMapperClass(LineCountMapper.class);
        lineCountJob.setReducerClass(LineCountReducer.class);
        lineCountJob.setOutputKeyClass(Text.class);
        lineCountJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(lineCountJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(lineCountJob, new Path(args[1] + "/lineCount"));
        lineCountJob.waitForCompletion(true);

        // Word Count Job
        Job wordCountJob = Job.getInstance(new Configuration(), "Word Count");
        wordCountJob.setJarByClass(TextAnalysisDriver.class);
        wordCountJob.setMapperClass(WordCountMapper.class);
        wordCountJob.setReducerClass(WordCountReducer.class);
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1] + "/wordCount"));
        wordCountJob.waitForCompletion(true);

        // Average Words per Line Job
        Job avgWordsJob = Job.getInstance(new Configuration(), "Average Words Per Line");
        avgWordsJob.setJarByClass(TextAnalysisDriver.class);
        avgWordsJob.setMapperClass(AvgWordsPerLineMapper.class);
        avgWordsJob.setReducerClass(AvgWordsPerLineReducer.class);
        avgWordsJob.setOutputKeyClass(Text.class);
        avgWordsJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(avgWordsJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(avgWordsJob, new Path(args[1] + "/avgWordsPerLine"));
        System.exit(avgWordsJob.waitForCompletion(true) ? 0 : 1);
    }
}
