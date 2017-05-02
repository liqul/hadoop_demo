package tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.EnumSet;

/**
 * Created by llq on 17-2-28.
 */
public class SeqAppender extends Configured implements Tool {
    Configuration conf = null;
    public int run(String[] strings) throws Exception {
        conf = this.getConf();
        SequenceFile.Writer writer = SequenceFile.createWriter(
                FileContext.getFileContext(this.conf),
                this.conf,
                new Path("/tmp/sequencefile"),
                Text.class,
                Text.class,
                SequenceFile.CompressionType.NONE,
                null,
                new SequenceFile.Metadata(),
                EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND),
                Options.CreateOpts.blockSize(64 * 1024 * 1024));
        writer.append(new Text("hello"), new Text("world"));
        writer.hsync();
        writer.close();
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SeqAppender(), args);
        System.exit(exitCode);
    }
}
