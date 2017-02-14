import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by llq on 17-2-13.
 */
public class SimpleRowCounter extends Configured implements Tool {
    static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {
        public static enum Counters {ROWS}
        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) {
            context.getCounter(Counters.ROWS).increment(1L);
        }
    }

    public int run(String[] args) throws Exception {
        String tableName = args[0];
        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());
        scan.setCaching(500);

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SimpleRowCounter.class);
        TableMapReduceUtil.initTableMapperJob(
                tableName,
                scan,
                RowCounterMapper.class,
                ImmutableBytesWritable.class,
                Result.class,
                job);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);
        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(HBaseConfiguration.create(), new SimpleRowCounter(), args);
        System.exit(exitCode);
    }
}
