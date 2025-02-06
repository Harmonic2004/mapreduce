import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RatingJob {
    public static void main(String[] args) throws Exception {
        // Cấu hình job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Rating Average");
        job.setJarByClass(RatingJob.class);

        // Đặt Mapper và Reducer
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        // Đặt kiểu dữ liệu cho output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Đặt input và output
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Chạy job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
