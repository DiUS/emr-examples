package au.com.dius.emr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CommonCrawlMapper extends Mapper<Text, Text, Text, LongWritable> {

  private final static LongWritable one = new LongWritable(1);

  private String[] targetWords;
  private Text word = new Text();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    targetWords = context.getConfiguration().get("target.words").split(",");
  }

  public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    for (String targetWord: targetWords) {
      StringTokenizer tokenizer = new StringTokenizer(value.toString());
      while (tokenizer.hasMoreTokens()) {
        if (targetWord.equalsIgnoreCase(tokenizer.nextToken())) {
          word.set(targetWord);
          context.write(word, one);
        }
      }
    }
  }

}
