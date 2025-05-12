import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class MapSidePartitionedJoin {

    public static class JoinMapper
            extends Mapper<LongWritable, TupleWritable, NullWritable, Text> {

        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, TupleWritable tuple, Context ctx)
                throws IOException, InterruptedException {

            // tuple.get(0) -> customer record, tuple.get(1) -> order record
            if (tuple.size() == 2 && tuple.get(0) != null && tuple.get(1) != null) {
                String joined = ((Text) tuple.get(0)).toString() + "|" +
                        ((Text) tuple.get(1)).toString();
                outVal.set(joined);
                ctx.write(NullWritable.get(), outVal);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String expr = CompositeInputFormat.compose(
                "inner",
                SequenceFileInputFormat.class,
                new Path(args[1]),
                new Path(args[2]));
        conf.set("mapreduce.join.expr", expr);

        Job job = Job.getInstance(conf, "Map-side join: customer ⨝ orders");
        job.setJarByClass(MapSidePartitionedJoin.class);

        job.setInputFormatClass(CompositeInputFormat.class);
        job.setMapperClass(JoinMapper.class);
        job.setNumReduceTasks(0);                 // map-only!

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3])); // /tpch/output/join

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}