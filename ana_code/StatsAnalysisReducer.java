import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StatsAnalysisReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        ArrayList<Double> nums = new ArrayList<>();
        double sum = 0;

        for (DoubleWritable val : values) {
            double num = val.get();
            nums.add(num);
            sum += num;
        }

        int n = nums.size();

        if (n == 0) {
            return;
        }

        // Mean
        double mean = sum / n;

        // Median
        Collections.sort(nums);
        double median;
        if (n % 2 == 0) {
            median = (nums.get(n / 2 - 1) + nums.get(n / 2)) / 2.0;
        } else {
            median = nums.get(n / 2);
        }

        // Mode
        HashMap<Double, Integer> freq = new HashMap<>();
        double mode = nums.get(0);
        int maxCount = 0;

        for (double num : nums) {
            int count = freq.getOrDefault(num, 0) + 1;
            freq.put(num, count);

            if (count > maxCount) {
                maxCount = count;
                mode = num;
            }
        }

        String output =
                "Mean=" + mean +
                ", Median=" + median +
                ", Mode=" + mode;

        context.write(key, new Text(output));
    }
}