package msg.kissa2.exercise.batch.data;


import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data

public abstract class Table {
    public String name;
    public List<String> schema;
    public List<Record> records;
    // NOTE: implement flyweight pattern to explore column names and data types

    public Table() {
        buildStructure();
    }

    protected abstract void buildStructure();

    public void insertRecord(Record record) {
        if (!verify(record)) {
            throw new RuntimeException("Invalid schema!");
        }
        records.add(record);
    }

    private boolean verify(Record record) {
        for (String key : record.columns.keySet()) {
           if (!schema.contains(key)) {
               return  false;
           }
        }
        return  true;
    }

    public static class Record {
        public Map<String, String> columns;

        public void setAttribute(String name, String value) {
            columns.put(name, value);
        }

        public List<String> getValues() {
            return columns.values().stream().collect(Collectors.toList());
        }
    }
}
