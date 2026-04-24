import java.io.IOException;
import java.io.StringReader;

import javax.naming.Context;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SeasonStatsMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        try {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(new StringReader(line));

            for (CSVRecord record : records) {

                String year;
                String season;
                String showName = "";
                double attendance;
                double capacity;
                double gross;


                // FIX: Initial version assumed all cleaned data had same schema
                // NOTE: Mapper handles cleaned data format produced by ETL stage
                // Format: year,season,showName,attendance,capacity,gross
                if (record.size() >= 6) {
                    year = record.get(0).trim();
                    season = record.get(1).trim();
                    showName = record.get(2).trim();
                    attendance = Double.parseDouble(record.get(3).trim());
                    capacity = Double.parseDouble(record.get(4).trim());
                    gross = Double.parseDouble(record.get(5).trim());

                    boolean isBigShow = showName.equalsIgnoreCase("Wicked")
                            || showName.equalsIgnoreCase("The Lion King");

                    String baseKey = year + "_" + season;

                    context.write(new Text("ALL_" + baseKey + "_ATT"), new DoubleWritable(attendance));
                    context.write(new Text("ALL_" + baseKey + "_CAP"), new DoubleWritable(capacity));
                    context.write(new Text("ALL_" + baseKey + "_GROSS"), new DoubleWritable(gross));

                    // FILTER: Excluding major grossing shows (Wicked, Lion King) for NO_BIG analysis
                    if (!isBigShow) {
                        context.write(new Text("NO_BIG_" + baseKey + "_ATT"), new DoubleWritable(attendance));
                        context.write(new Text("NO_BIG_" + baseKey + "_CAP"), new DoubleWritable(capacity));
                        context.write(new Text("NO_BIG_" + baseKey + "_GROSS"), new DoubleWritable(gross));
                    }

                // Format for recent data: year,season,attendance,capacity,gross
                } else if (record.size() == 5) {
                    year = record.get(0).trim();
                    season = record.get(1).trim();
                    attendance = Double.parseDouble(record.get(2).trim());
                    capacity = Double.parseDouble(record.get(3).trim());
                    gross = Double.parseDouble(record.get(4).trim());

                    String baseKey = year + "_" + season;

                    context.write(new Text("ALL_" + baseKey + "_ATT"), new DoubleWritable(attendance));
                    context.write(new Text("ALL_" + baseKey + "_CAP"), new DoubleWritable(capacity));
                    context.write(new Text("ALL_" + baseKey + "_GROSS"), new DoubleWritable(gross));
                }
            }

        } catch (Exception e) {
            // skip malformed rows
        }
    }
}