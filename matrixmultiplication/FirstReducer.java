import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FirstReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> mValues = new ArrayList<>();
        ArrayList<String> nValues = new ArrayList<>();

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts[0].equals("m")) {
                mValues.add(val.toString());
            } else if (parts[0].equals("n")) {
                nValues.add(val.toString());
            }
        }

	for (String m : mValues) {
	    String[] ele_m = m.split(",");
	    for (String n : nValues) {
		String[] ele_n = n.split(",");
	        context.write(key, new Text(ele_m[1] + "," + ele_n[1] + "," + Integer.parseInt(ele_m[2]) * Integer.parseInt(ele_n[2])));
	    }
	}
    }
}

