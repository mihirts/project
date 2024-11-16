import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AvgWordsPerLineReducer extends Reducer<Text, IntWritable, Text, Text> {
    private int totalLines = 0;
    private int totalWords = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        if (key.toString().equals("TotalLines")) {
            for (IntWritable value : values) {
                totalLines += value.get();
            }
        } else if (key.toString().equals("TotalWords")) {
            for (IntWritable value : values) {
                totalWords += value.get();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        double avgWordsPerLine = totalLines > 0 ? (double) totalWords / totalLines : 0.0;
        context.write(new Text("AverageWordsPerLine"), new Text(String.format("%.2f", avgWordsPerLine)));
    }
}
