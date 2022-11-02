package msg.kissa2.exercise.batch.writer;
import com.opencsv.CSVWriter;
import msg.kissa2.exercise.batch.data.Table;
import org.springframework.batch.item.ItemWriter;
import java.io.*;
import java.util.ArrayList;
import java.util.List;


public class CsvTableWriter implements ItemWriter<Table> {

    private Writer writer;

    public CsvTableWriter() {
    }

    private void doWrite(Table table) {
        File file = new File( table.getName() + ".csv");

        if (!file.exists()) {
            try {
                file.createNewFile();
                // write header / schema
                CSVWriter csvWriter = new CSVWriter(new FileWriter(file));
                List<String> schema = table.getSchema();
                csvWriter.writeNext(schema.toArray(new String[schema.size()]));
                csvWriter.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                // append records
                CSVWriter csvWriter = new CSVWriter(new FileWriter(file, true));
                List<Table.Record> records = table.getRecords();

                for (Table.Record r : records) {
                    List<String> vals = r.getValues();
                    String[] row = new String[vals.size()];
                    new ArrayList<>(vals).toArray(row);
                    csvWriter.writeNext(row);
                }
                csvWriter.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void write(List<? extends Table> items) throws Exception {
        for(Table t : items) {
            doWrite(t);
        }
    }
}
