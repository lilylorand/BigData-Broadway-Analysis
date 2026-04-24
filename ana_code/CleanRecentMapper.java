import java.io.IOException;
import java.io.StringReader;

import javax.naming.Context;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanRecentMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header
        if (line.toLowerCase().contains("date.full")) {
            return;
        }

        try {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(new StringReader(line));

            for (CSVRecord record : records) {
                // Expected:
                // Date.day,Date.full,Date.Month,Date.Year,Statistics.Attendance,Statistics.Capacity,Statistics.Gross
                if (record.size() < 7) {
                    return;
                }

                String monthStr = record.get(2).trim();
                String year = record.get(3).trim();

                // FIX: 2016–2026 dataset contains commas in numeric fields (e.g., "294,349")
                // Removed before parsing to avoid incorrect values
                String attendanceStr = record.get(4).trim().replace(",", "");
                String capacityStr = record.get(5).trim().replace(",", "");
                String grossStr = record.get(6).trim().replace(",", "");

                double attendance = Double.parseDouble(attendanceStr);
                double capacity = Double.parseDouble(capacityStr);
                double gross = Double.parseDouble(grossStr);

                int month = Integer.parseInt(monthStr);

                String season;
                if (month == 6 || month == 7 || month == 8) {
                    season = "summer";
                } else if (month == 11 || month == 12 || month == 1) {
                    season = "winter";
                } else {
                    return;
                }

                String csv = year + "," + season + "," + attendance + "," + capacity + "," + gross;

                context.write(new Text(csv), new Text("1"));
            }

        } catch (Exception e) {
            // Skip malformed rows
        }
    }
}