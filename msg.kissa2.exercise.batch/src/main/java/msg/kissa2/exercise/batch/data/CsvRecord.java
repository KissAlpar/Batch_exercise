package msg.kissa2.exercise.batch.data;

import java.util.List;

public class CsvRecord {
    private List<String> list;

    public CsvRecord(List<String> list) {
        this.list = list;
    }
    
    public List<String> getList() {
    	return this.list;
    }
    
    public void setList(List<String> list) {
    	this.list = list;
    }
}
