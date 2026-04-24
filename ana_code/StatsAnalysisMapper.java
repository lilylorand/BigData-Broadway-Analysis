import java.io.IOException;
import java.io.StringReader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StatsAnalysisMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        try {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(new StringReader(line));

            for (CSVRecord record : records) {
                if (record.size() < 6) {
                    return;
                }

                double attendance = Double.parseDouble(record.get(2));
                double capacity = Double.parseDouble(record.get(3));
                double gross = Double.parseDouble(record.get(4));

                context.write(new Text("Attendance"), new DoubleWritable(attendance));
                context.write(new Text("Capacity"), new DoubleWritable(capacity));
                context.write(new Text("Gross"), new DoubleWritable(gross));
            }

        } catch (Exception e) {
            // skip malformed rows
        }
    }
}