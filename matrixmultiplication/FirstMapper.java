import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FirstMapper extends Mapper<Object, Text, IntWritable, Text> {
	private String currentMatrix="";
	private int rowIndex=0;

	@Override
    	protected void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {
		String line = value.toString().trim();
		if (line.equals("M") || line.equals("N")) {
			currentMatrix = line;
			rowIndex=0;
			return;
		}

		String[] elements = line.split("\\s+");
		rowIndex++;
		
		for (int colIndex = 0; colIndex < elements.length; colIndex++) {
         	    int valueInt = Integer.parseInt(elements[colIndex]);
	
        	    if (currentMatrix.equals("M")) {
                // Phát dữ liệu ma trận M: (j, (m, i, mij))
                    context.write(new IntWritable(colIndex + 1), 
                              new Text("m," + rowIndex + "," + valueInt));
            	    } else if (currentMatrix.equals("N")) {
                // Phát dữ liệu ma trận N: (j, (n, k, njk))
                    context.write(new IntWritable(rowIndex), 
                              new Text("n," + (colIndex + 1) + "," + valueInt));
            	    }
        	}
	}
}
