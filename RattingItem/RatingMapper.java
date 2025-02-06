import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RatingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text userItemPair = new Text();
    private IntWritable rating = new IntWritable();
    private boolean isHeader = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Nếu là dòng tiêu đề, bỏ qua
        if (isHeader) {
            isHeader = false;
            return;
        }

	String[] parts = value.toString().split(",");

        if (parts.length == 4) {
            String userID = parts[0];
            String itemID = parts[1];
            int rate = Integer.parseInt(parts[2]);

            userItemPair.set(userID + "-" + itemID);  // Combine user and item IDs as key
            rating.set(rate);  // Set the rating as value

            context.write(userItemPair, rating);  // Emit key-value pair
        }
    }
}
