import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class AvgWordsPerLineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static Text wordKey = new Text("TotalWords");
    private final static Text lineKey = new Text("TotalLines");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(lineKey, new IntWritable(1));  // Each line adds '1' to line count

        int wordCount = new StringTokenizer(value.toString()).countTokens();
        context.write(wordKey, new IntWritable(wordCount));  // Add word count of each line
    }
}
