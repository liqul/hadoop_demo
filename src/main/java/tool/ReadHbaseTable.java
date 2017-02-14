package tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by llq on 17-2-13.
 */
public class ReadHbaseTable extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(HBaseConfiguration.create(), new ReadHbaseTable(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        TableName tableName = TableName.valueOf(args[0]);
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(args[1]));
        Result result = table.get(get);
        byte[] cf = Bytes.toBytes(args[2]);
        byte[] qualifier = Bytes.toBytes(args[3]);
        System.out.println("Get: " + args[0] + "," + args[1] + "," + args[2] + "," + args[3] + "-->" + result.getValue(cf, qualifier).length + " Bytes");
        return 0;
    }
}
