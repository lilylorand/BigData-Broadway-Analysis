import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SeasonStatsReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double sum = 0.0;
        int count = 0;

        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }

        if (count == 0) {
            return;
        }

        double average = sum / count;

        // FIX: Earlier output formatting was inconsistent
        // Reducer now outputs standardized "Average=value" format
        context.write(key, new Text("Average=" + average));
    }
}