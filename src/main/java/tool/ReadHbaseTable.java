package tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by llq on 17-2-13.
 */
public class ReadHbaseTable {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        TableName tableName = TableName.valueOf("TeraSort");
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes("part0"));
        Result result = table.get(get);
        System.out.println("Get: " + result.getValue(Bytes.toBytes("data"), Bytes.toBytes("content")).length);
    }
}
