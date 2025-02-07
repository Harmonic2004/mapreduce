import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class SecondMapper extends Mapper<LongWritable, Text, Text, LongWritable> {        
        @Override
        protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
		String[] parts = value.toString().split("\\s+")[1].split(",");
		context.write(new Text(parts[0] + "," + parts[1]), new LongWritable(Long.parseLong(parts[2])));
        }
}
