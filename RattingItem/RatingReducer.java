import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RatingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable averageRating = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;

        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }

        int average = sum / count;  // Tính trung bình điểm
        averageRating.set(average);

        context.write(key, averageRating);  // Emit kết quả (user-item, averageRating)
    }
}
