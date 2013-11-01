package au.com.dius.emr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CommonCrawlTool extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CommonCrawlTool(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();

    Job job = new Job(conf, "Word count");
    job.setJarByClass(CommonCrawlTool.class);

    job.setMapperClass(CommonCrawlMapper.class);
    job.setReducerClass(CommonCrawlReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    String[] segments = job.getConfiguration().get("crawl.segments").split(",");
    Path[] paths = new Path[segments.length];
    for (int i = 0; i < segments.length; i++) {
      paths[i] = new Path("/common-crawl/parse-output/segment/" + segments[i]);
    }
    SequenceFileInputFormat.setInputPaths(job, paths);

    FileOutputFormat.setOutputPath(job, new Path("/output"));

    return job.waitForCompletion(true) ? 0 : 1;
  }
}
