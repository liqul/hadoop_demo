package tool;

import format.CombineWholeFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by llq on 17-2-13.
 */
public class HbaseTeraImporter extends Configured implements Tool {

    static class HbaseTeraMapper extends Mapper<Text, BytesWritable, NullWritable, Put> {
        public static final byte[] cf = "data".getBytes();
        public static final byte[] attr1 = "content".getBytes();

        @Override
        public void map(Text fileName, BytesWritable data, Context context) throws IOException,
                InterruptedException {
            byte[] rowKey = fileName.getBytes();
            Put p = new Put(rowKey);

            //noinspection Since15
            p.add(cf, attr1, data.copyBytes());
            System.out.println("insert " + data.getLength());
            context.write(null, p);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(HbaseTeraImporter.class);
        CombineWholeFileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(CombineWholeFileInputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, args[1]);
        job.setMapperClass(HbaseTeraMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(TableOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(HBaseConfiguration.create(), new HbaseTeraImporter(), args);
        System.exit(exitCode);
    }
}
