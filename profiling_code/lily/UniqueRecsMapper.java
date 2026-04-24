import java.io.IOException;
import java.io.StringReader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueRecsMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header
        if (line.contains("Date.Day")) {
            return;
        }

        try {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(new StringReader(line));

            for (CSVRecord record : records) {
                int size = record.size();

                String monthOrSeason;
                String attendance;
                String capacity;
                String gross;
                String soldOut = null;

                if (size >= 10) {
                    // original broadway.csv format
                    monthOrSeason = record.get(2).trim();
                    attendance = record.get(7).trim();
                    capacity = record.get(8).trim();
                    gross = record.get(9).trim();
                } else if (size >= 6) {
                    // cleaned format: year,season,attendance,capacity,gross,soldOut
                    monthOrSeason = record.get(1).trim();
                    attendance = record.get(2).trim();
                    capacity = record.get(3).trim();
                    gross = record.get(4).trim();
                    soldOut = record.get(5).trim();
                } else {
                    return;
                }

                context.write(new Text("MonthOrSeason=" + monthOrSeason), one);
                context.write(new Text("Attendance=" + attendance), one);
                context.write(new Text("Capacity=" + capacity), one);
                context.write(new Text("Gross=" + gross), one);

                if (soldOut != null) {
                    context.write(new Text("SoldOut=" + soldOut), one);
                }
            }

        } catch (Exception e) {
            // skip bad rows
        }
    }
}