import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by llq on 17-2-13.
 */
public class TeraSeqParserMapper extends Mapper<Text, BytesWritable, Text, Text> {
    private Text outKey = new Text();
    private Text outVal = new Text();

    public void map(Text dummy, BytesWritable value, Context context) throws IOException,
            InterruptedException {
        byte[] content = value.copyBytes();
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
