package au.com.dius.emr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommonCrawlReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

  public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    int sum = 0;
    for (LongWritable val : values) {
      sum += val.get();
    }
    context.write(key, new LongWritable(sum));
  }

}
