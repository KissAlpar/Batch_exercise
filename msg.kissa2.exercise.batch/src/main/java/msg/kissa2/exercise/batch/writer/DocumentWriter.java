package msg.kissa2.exercise.batch.writer;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.PreDestroy;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.batch.item.ItemWriter;
import msg.kissa2.exercise.batch.data.CsvRecord;
import msg.kissa2.exercise.batch.data.Timpl100;
import msg.kissa2.exercise.batch.data.Timpl101;
import msg.kissa2.exercise.batch.data.Timpl102;

public class DocumentWriter implements ItemWriter<CsvRecord>, Closeable {

	private CSVPrinter printer1;
	private CSVPrinter printer2;
	private CSVPrinter printer3;
	
	public DocumentWriter() throws IOException {
		printer1 = new CSVPrinter(new FileWriter("TIMPL100.csv"), CSVFormat.DEFAULT);
		printer1.printRecord(new Timpl100().getSchema().toArray());
		printer2 = new CSVPrinter(new FileWriter("TIMPL101.csv"), CSVFormat.DEFAULT);
		printer2.printRecord(new Timpl101().getSchema().toArray());
		printer3 = new CSVPrinter(new FileWriter("TIMPL102.csv"), CSVFormat.DEFAULT);
		printer3.printRecord(new Timpl102().getSchema().toArray());
	}
	
	@Override
	public void write(List<? extends CsvRecord> items) throws Exception {
		for (int i = 0; i < items.size(); i++) {
			ArrayList<String> data = new ArrayList<String>(items.get(i).getList());
			String name = data.get(0);
			data.remove(0);
			if (name.equals("TIMPL100")) {
					printer1.printRecord(data);
			} else if (name.equals("TIMPL101")) {
					printer2.printRecord(data);
			} else if (name.equals("TIMPL102")){
					printer3.printRecord(data);
			}
		}
	}

	@PreDestroy
	@Override
	public void close() throws IOException {
		printer1.close();
		printer2.close();
		printer3.close();
	}
}