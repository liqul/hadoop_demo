//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;

import format.CombineTeraInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.terasort.TeraInputFormat;
import org.apache.hadoop.examples.terasort.TeraOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyTeraSort extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(MyTeraSort.class);
    static String SIMPLE_PARTITIONER = "mapreduce.terasort.simplepartitioner";
    static String OUTPUT_REPLICATION = "mapreduce.terasort.output.replication";

    public MyTeraSort() {
    }

    public static boolean getUseSimplePartitioner(JobContext job) {
        return job.getConfiguration().getBoolean(SIMPLE_PARTITIONER, false);
    }

    public static void setUseSimplePartitioner(Job job, boolean value) {
        job.getConfiguration().setBoolean(SIMPLE_PARTITIONER, value);
    }

    public static int getOutputReplication(JobContext job) {
        return job.getConfiguration().getInt(OUTPUT_REPLICATION, 1);
    }

    public static void setOutputReplication(Job job, int value) {
        job.getConfiguration().setInt(OUTPUT_REPLICATION, value);
    }

    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        LOG.info("starting");
        //common settings
        Configuration conf = this.getConf();
        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        Job job = Job.getInstance(conf);
        Path outputDir = new Path(args[2]);
        job.setJobName("TeraSort");
        job.setJarByClass(MyTeraSort.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TeraOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputDir);
        Path partitionFile = new Path(outputDir, "_partition.lst");
        FileSystem fs = FileSystem.get(this.getConf());

        if(args[0].equals("fs")) {
            System.out.println("file system mode");
            Path inputDir = new Path(args[1]);
            CombineTeraInputFormat.setInputPaths(job, new Path[]{inputDir});
            job.setInputFormatClass(CombineTeraInputFormat.class);
        } else if (args[0].equals("hb")) {
            System.out.println("hbase mode");
            if(!fs.exists(partitionFile)) {
                System.out.println("No partition file found. Exit");
                return -1;
            }
            String tableName = args[1];
            Scan scan = new Scan();
            scan.setCaching(10);        // 1 is the default in Scan, which will be bad for MapReduce jobs
            scan.setCacheBlocks(false);  // don't set to true for MR jobs
            scan.setFilter(new FirstKeyOnlyFilter());
            System.out.println("init job");
            TableMapReduceUtil.initTableMapperJob(
                    tableName,        // input HBase table name
                    scan,             // Scan instance to control CF and attribute selection
                    TeraParserMapper.class,   // mapper
                    Text.class,             // mapper output key
                    Text.class,             // mapper output value
                    job);
        } else if (args[0].equals("sq")) {
            if(!fs.exists(partitionFile)) {
                System.out.println("No partition file found. Exit");
                return -1;
            }
            System.out.println("sequence file mode");
            Path inputDir = new Path(args[1]);
            FileInputFormat.addInputPath(job, inputDir);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapperClass(TeraSeqParserMapper.class);
        }


        URI partitionUri = new URI(partitionFile.toString() + "#" + "_partition.lst");

        //create the partition file only if not exists.
        if(!fs.exists(partitionFile)) {
            long ret = System.currentTimeMillis();
            try {
                TeraInputFormat.writePartitionFile(job, partitionFile);
            } catch (Throwable var12) {
                LOG.error(var12.getMessage());
                return -1;
            }
            long end = System.currentTimeMillis();
            System.out.println("Spent " + (end - ret) + "ms computing partitions.");
        } else {
            System.out.println("The partition file already exists. Skip generating it.");
        }

        job.addCacheFile(partitionUri);
        job.setPartitionerClass(MyTeraSort.TotalOrderPartitioner.class);

        job.getConfiguration().setInt("dfs.replication", getOutputReplication(job));
        //copied from TeraOutputFormat
        job.getConfiguration().setBoolean("mapreduce.terasort.final.sync", true);
        System.out.println("run & wait");
        int ret1 = job.waitForCompletion(true)?0:1;
        LOG.info("done");
        return ret1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MyTeraSort(), args);
        System.exit(res);
    }

    public static class SimplePartitioner extends Partitioner<Text, Text> implements Configurable {
        int prefixesPerReduce;
        private static final int PREFIX_LENGTH = 3;
        private Configuration conf = null;

        public SimplePartitioner() {
        }

        public void setConf(Configuration conf) {
            this.conf = conf;
            this.prefixesPerReduce = (int)Math.ceil((double)(1.6777216E7F / (float)conf.getInt("mapreduce.job.reduces", 1)));
        }

        public Configuration getConf() {
            return this.conf;
        }

        public int getPartition(Text key, Text value, int numPartitions) {
            byte[] bytes = key.getBytes();
            int len = Math.min(3, key.getLength());
            int prefix = 0;

            for(int i = 0; i < len; ++i) {
                prefix = prefix << 8 | 255 & bytes[i];
            }

            return prefix / this.prefixesPerReduce;
        }
    }

    static class TotalOrderPartitioner extends Partitioner<Text, Text> implements Configurable {
        private MyTeraSort.TotalOrderPartitioner.TrieNode trie;

        private Text[] splitPoints;
        private Configuration conf;

        private static Text[] readPartitions(FileSystem fs, Path p, Configuration conf) throws IOException {
            int reduces = conf.getInt("mapreduce.job.reduces", 1);
            Text[] result = new Text[reduces - 1];
            FSDataInputStream reader = fs.open(p);

            for(int i = 0; i < reduces - 1; ++i) {
                result[i] = new Text();
                result[i].readFields(reader);
            }

            reader.close();
            return result;
        }

        private static MyTeraSort.TotalOrderPartitioner.TrieNode buildTrie(Text[] splits, int lower, int upper, Text prefix, int maxDepth) {
            int depth = prefix.getLength();
            if(depth < maxDepth && lower != upper) {
                MyTeraSort.TotalOrderPartitioner.InnerTrieNode result = new MyTeraSort.TotalOrderPartitioner.InnerTrieNode(depth);
                Text trial = new Text(prefix);
                trial.append(new byte[1], 0, 1);
                int currentBound = lower;

                for(int ch = 0; ch < 255; ++ch) {
                    trial.getBytes()[depth] = (byte)(ch + 1);

                    for(lower = currentBound; currentBound < upper && splits[currentBound].compareTo(trial) < 0; ++currentBound) {
                        ;
                    }

                    trial.getBytes()[depth] = (byte)ch;
                    result.child[ch] = buildTrie(splits, lower, currentBound, trial, maxDepth);
                }

                trial.getBytes()[depth] = -1;
                result.child[255] = buildTrie(splits, currentBound, upper, trial, maxDepth);
                return result;
            } else {
                return new MyTeraSort.TotalOrderPartitioner.LeafTrieNode(depth, splits, lower, upper);
            }
        }

        public void setConf(Configuration conf) {
            try {
                LocalFileSystem ie = FileSystem.getLocal(conf);
                this.conf = conf;
                Path partFile = new Path("_partition.lst");
                this.splitPoints = readPartitions(ie, partFile, conf);
                this.trie = buildTrie(this.splitPoints, 0, this.splitPoints.length, new Text(), 2);
            } catch (IOException var4) {
                throw new IllegalArgumentException("can\'t read partitions file", var4);
            }
        }

        public Configuration getConf() {
            return this.conf;
        }

        public TotalOrderPartitioner() {
        }

        public int getPartition(Text key, Text value, int numPartitions) {
            return this.trie.findPartition(key);
        }

        static class LeafTrieNode extends MyTeraSort.TotalOrderPartitioner.TrieNode {
            int lower;
            int upper;
            Text[] splitPoints;

            LeafTrieNode(int level, Text[] splitPoints, int lower, int upper) {
                super(level);
                this.splitPoints = splitPoints;
                this.lower = lower;
                this.upper = upper;
            }

            int findPartition(Text key) {
                for(int i = this.lower; i < this.upper; ++i) {
                    if(this.splitPoints[i].compareTo(key) > 0) {
                        return i;
                    }
                }

                return this.upper;
            }

            void print(PrintStream strm) throws IOException {
                for(int i = 0; i < 2 * this.getLevel(); ++i) {
                    strm.print(' ');
                }

                strm.print(this.lower);
                strm.print(", ");
                strm.println(this.upper);
            }
        }

        static class InnerTrieNode extends MyTeraSort.TotalOrderPartitioner.TrieNode {
            private MyTeraSort.TotalOrderPartitioner.TrieNode[] child = new MyTeraSort.TotalOrderPartitioner.TrieNode[256];

            InnerTrieNode(int level) {
                super(level);
            }

            int findPartition(Text key) {
                int level = this.getLevel();
                return key.getLength() <= level?this.child[0].findPartition(key):this.child[key.getBytes()[level] & 255].findPartition(key);
            }

            void setChild(int idx, MyTeraSort.TotalOrderPartitioner.TrieNode child) {
                this.child[idx] = child;
            }

            void print(PrintStream strm) throws IOException {
                for(int ch = 0; ch < 256; ++ch) {
                    for(int i = 0; i < 2 * this.getLevel(); ++i) {
                        strm.print(' ');
                    }

                    strm.print(ch);
                    strm.println(" ->");
                    if(this.child[ch] != null) {
                        this.child[ch].print(strm);
                    }
                }

            }
        }

        abstract static class TrieNode {
            private int level;

            TrieNode(int level) {
                this.level = level;
            }

            abstract int findPartition(Text var1);

            abstract void print(PrintStream var1) throws IOException;

            int getLevel() {
                return this.level;
            }
        }
    }
}
