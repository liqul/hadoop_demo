import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by llq on 17-2-13.
 */
public class TeraParserMapper extends TableMapper<Text, Text> {
    public static final byte[] cf = "data".getBytes();
    public static final byte[] attr1 = "content".getBytes();
    private Text outKey = new Text();
    private Text outVal = new Text();

    public void map(ImmutableBytesWritable rowKey, Result value, Context context) throws IOException,
            InterruptedException {
        byte[] content = value.getValue(cf, attr1);
        for(int i = 0; i < content.length; i += 100) {
            //noinspection Since15
            byte[] key = Arrays.copyOfRange(content, i, i+10);
            //noinspection Since15
            byte[] val = Arrays.copyOfRange(content, i+10, i+100);
            outKey.set(key);
            outVal.set(val);
            context.write(outKey, outVal);
        }
    }
}
