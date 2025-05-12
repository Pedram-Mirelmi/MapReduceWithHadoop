import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class Sorter {

    public static class CustSortMapper
            extends Mapper<LongWritable, Text, LongWritable, Text> {

        private final LongWritable outKey = new LongWritable();
        private final Text          outVal = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context ctx)
                throws IOException, InterruptedException {

            String[] fields = line.toString().split("\\|", -1);
            long custKey = Long.parseLong(fields[0]);
            outKey.set(custKey);
            // remove trailing empty column after last '|'
            outVal.set(line.toString().substring(0, line.getLength() - 1));
            ctx.write(outKey, outVal);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sort customers by custkey");
        job.setJarByClass(Sorter.class);

        job.setMapperClass(CustSortMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));   // raw .tbl
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // e.g. /tpch/sorted/customer
    job.setNumReduceTasks(Integer.parseInt(args[3]));       // same N everywhere
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}