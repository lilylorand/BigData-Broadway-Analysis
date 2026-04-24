import java.io.IOException;
import java.io.StringReader;

import javax.naming.Context;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanWithShowMapper extends Mapper<Object, Text, Text, Text> {

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
                if (record.size() < 10) {
                    return;
                }

                // FIX: Previous cleaner removed show names, preventing filtering
                // Updated version keeps show names for NO_BIG analysis
                String monthStr = record.get(2).trim();
                String year = record.get(3).trim();
                String showName = record.get(4).trim();
                String attendance = record.get(7).trim();
                String capacity = record.get(8).trim();
                String gross = record.get(9).trim();

                int month = Integer.parseInt(monthStr);

                String season;
                if (month == 6 || month == 7 || month == 8) {
                    season = "summer";
                } else if (month == 11 || month == 12 || month == 1) {
                    season = "winter";
                } else {
                    return;
                }

                String csv = year + "," + season + "," + showName + "," + attendance + "," + capacity + "," + gross;

                context.write(new Text(csv), new Text("1"));
            }

        } catch (Exception e) {
            // Skip bad rows that cause parsing errors
        }
    }
}
