package format;

import org.apache.hadoop.examples.terasort.TeraInputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/**
 * Created by llq on 17-2-10.
 */
public class CombineTeraInputFormat extends CombineFileInputFormat<Text, Text> {
    public CombineTeraInputFormat() {
        super();
        setMaxSplitSize(67108864 * 2); // 128 MB
    }

    public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException {
        return new CombineFileRecordReader<Text, Text>(
                (CombineFileSplit)inputSplit,
                taskAttemptContext,
                TeraRecordReader.class
        );
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        System.out.println("Enter getSplits");
        List<InputSplit> splits = super.getSplits(job);
        System.out.println("Leave getSplits");
        return splits;
    }


    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    //copied from TeraInputFormat
    static class TeraRecordReader extends RecordReader<Text, Text> {
        private FSDataInputStream in;
        private long offset;
        private long length;
        private static final int RECORD_LENGTH = 100;
        private byte[] buffer = new byte[100];
        private Text key;
        private Text value;

        public TeraRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
            FileSplit fileSplit = new FileSplit(split.getPath(index),
                    split.getOffset(index), split.getLength(index),
                    split.getLocations());

            Path p = fileSplit.getPath();
            FileSystem fs = p.getFileSystem(context.getConfiguration());
            this.in = fs.open(p);
            long start = fileSplit.getStart();
            this.offset = (100L - start % 100L) % 100L;
            this.in.seek(start + this.offset);
            this.length = fileSplit.getLength();
        }


        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        }

        public void close() throws IOException {
            this.in.close();
        }

        public Text getCurrentKey() {
            return this.key;
        }

        public Text getCurrentValue() {
            return this.value;
        }

        public float getProgress() throws IOException {
            return (float)this.offset / (float)this.length;
        }

        public boolean nextKeyValue() throws IOException {
            if(this.offset >= this.length) {
                return false;
            } else {
                long newRead;
                for(int read = 0; read < 100; read = (int)((long)read + newRead)) {
                    newRead = (long)this.in.read(this.buffer, read, 100 - read);
                    if(newRead == -1L) {
                        if(read == 0) {
                            return false;
                        }

                        throw new EOFException("read past eof");
                    }
                }

                if(this.key == null) {
                    this.key = new Text();
                }

                if(this.value == null) {
                    this.value = new Text();
                }

                this.key.set(this.buffer, 0, 10);
                this.value.set(this.buffer, 10, 90);
                this.offset += 100L;
                return true;
            }
        }
    }
}
