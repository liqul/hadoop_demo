package format;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by llq on 17-2-10.
 */
public class CombineWholeFileRecordReader extends RecordReader<Text, BytesWritable> {
    private WholeFileRecordReader reader;
    private String fileName;
    public CombineWholeFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException, InterruptedException {
        FileSplit fileSplit = new FileSplit(split.getPath(index),
                split.getOffset(index), split.getLength(index),
                split.getLocations());

        fileName = fileSplit.getPath().getName();

        reader = new WholeFileRecordReader();
        reader.initialize(fileSplit, context);
    }

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        return reader.nextKeyValue();
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return new Text(fileName);
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return reader.getCurrentValue();
    }

    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    public void close() throws IOException {
        reader.close();
    }
}
