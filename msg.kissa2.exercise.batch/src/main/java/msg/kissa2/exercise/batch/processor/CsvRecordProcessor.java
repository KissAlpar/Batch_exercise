package msg.kissa2.exercise.batch.processor;

import msg.kissa2.exercise.batch.data.*;
import org.springframework.batch.item.ItemProcessor;

import java.util.List;


// use this only with the first implementation of writer (CsvTableWriter)
public class CsvRecordProcessor implements ItemProcessor<CsvRecord, Table> {

    public CsvRecordProcessor() {}

    @Override
    public Table process(CsvRecord item) throws Exception {
        String tableName = item.getList().get(0);
        if (tableName.equals("TIMPL100")) {
            return buildTable(new Timpl100(), item.getList());
        } else if (tableName.equals("TIMPL101")) {
            return buildTable(new Timpl101(), item.getList());
        } else if (tableName.equals("TIMPL102")) {
            return buildTable(new Timpl102(), item.getList());
        }
        return null;
    }

    private Table buildTable(Table table, List<String> fields) {
        Table.Record rec = new Table.Record();
        for (int i = 0; i < table.schema.size(); i++) {
            String key = table.getSchema().get(i);
            String value = fields.get(i + 1); // skip table name
            rec.setAttribute(key, value);
        }
        table.insertRecord(rec);
        return table;
    }
}
