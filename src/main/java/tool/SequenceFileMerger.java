package tool;

import format.CombineWholeFileInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by llq on 17-2-10.
 */
public class SequenceFileMerger extends Configured implements Tool {

    static class SequenceMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {
        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf());

        job.setInputFormatClass(CombineWholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setMapperClass(SequenceMapper.class);

        job.setJarByClass(SequenceFileMerger.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SequenceFileMerger(), args);
        System.exit(exitCode);
    }
}
