package msg.kissa2.exercise.batch.data;

import java.util.List;

public class CsvRecord {
    private List<String> list;

    public CsvRecord(List<String> list) {
        this.list = list;
    }
    
    public CsvRecord(CsvRecord other) {
    	this.list = List.copyOf(other.list);
    }
    
    public List<String> getList() {
    	return this.list;
    }
    
    public void setList(List<String> list) {
    	this.list = list;
    }
    
    public Table.Record convertRecord(Table table) {
		Table.Record newRecord = new Table.Record();
		for (int i = 0; i < table.getSchema().size(); i++) {
			try {
				String key = table.getSchema().get(i);
				String value = getList().get(i);
				newRecord.setAttribute(key, value);
			} catch (IndexOutOfBoundsException e) { // maybe the record doesn't match the schema
				return null;
			}
			
		}
		return newRecord;
	}
}
