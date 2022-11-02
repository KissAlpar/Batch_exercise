package msg.kissa2.exercise.batch.data;

import lombok.Data;

import java.util.List;

public class CsvRecord {
    private final List<String> list;

    public CsvRecord(List<String> list) {
        this.list = list;
    }
    
    public List<String> getList() {
    	return this.list;
    }
}
