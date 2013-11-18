package au.com.dius.emr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

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
    job.setOutputValueClass(IntWritable.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, getPaths(conf));
    FileInputFormat.setInputPathFilter(job, CommonCrawlTextFilter.class);
    FileOutputFormat.setOutputPath(job, new Path("s3n://emr-examples.dius.com.au/output"));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  private Path[] getPaths(Configuration conf) throws IOException, URISyntaxException {
    Path segmentFile = new Path("s3n://aws-publicdatasets/common-crawl/parse-output/valid_segments.txt");
    BufferedReader reader = new BufferedReader(new InputStreamReader(segmentFile.getFileSystem(conf).open(segmentFile)));

    int maxSegments = Integer.parseInt(conf.get("max.segments"));
    String baseUri = conf.get("base.uri");

    List<String> segments = new ArrayList<String>();
    String line;

    while ((line = reader.readLine()) != null && segments.size() < maxSegments) {
      segments.add(line.replaceAll("\\n", "").replaceAll("\\s", ""));
    }
    reader.close();

    List<Path> paths = new ArrayList<Path>();
    for (String segment : segments) {
      paths.add(new Path(baseUri + "/parse-output/segment/" + segment));
    }

    return paths.toArray(new Path[paths.size()]);
  }

  static class CommonCrawlTextFilter implements PathFilter {

    public boolean accept(Path path) {
      if (path.toString().matches("^/common-crawl/parse-output/segment/\\d*$")) {
        return true;
      }
      return path.getName().matches(".*textData.*");
    }

  }
}

