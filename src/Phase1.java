import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class Phase1 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, IntWritable, DoubleWritable>{

    private IntWritable    movieId = new IntWritable();
    private DoubleWritable rating  = new DoubleWritable();

    private Configuration conf;
    private BufferedReader fis;

    @Override
    public void map(Object key, Text value, Context context)
                    throws IOException, InterruptedException {
      int indx = 0;
      String line = value.toString();
      StringTokenizer itr = new StringTokenizer(line, ":");
      while (itr.hasMoreTokens()) {
        String tok = itr.nextToken();
        if (indx == 1) {
            movieId.set(new Integer(tok));
        } else if (indx == 2) {
            rating.set(new Double(tok));
            context.write(movieId, rating);
        }
        indx++;
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    public void reduce(IntWritable key, Iterable<DoubleWritable> values,
                       Context context)
                       throws IOException, InterruptedException {
      double sum = 0;
      int count = 0;
      double avg;
      for (DoubleWritable val : values) {
        sum += val.get();
        count++;
      }
      avg = sum / count;
      result.set(avg);
      context.write(key, result);
    }

  }

  public static void run(String input, String output) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "lab3_phase1");
    job.setJarByClass(Phase1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    job.waitForCompletion(true);
  }
}
