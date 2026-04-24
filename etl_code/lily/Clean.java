import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Clean {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Clean Records");
    job.setJarByClass(Clean.class);
    job.setMapperClass(CleanMapper.class);
    job.setReducerClass(CleanReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class); // needs to be changed based on reduce output
    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(args[0]));  
    FileOutputFormat.setOutputPath(job, new Path(args[1])); 
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

