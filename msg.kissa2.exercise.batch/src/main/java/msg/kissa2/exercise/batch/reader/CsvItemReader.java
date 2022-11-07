package msg.kissa2.exercise.batch.reader;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import msg.kissa2.exercise.batch.data.CsvRecord;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;

public class CsvItemReader implements ItemReader<CsvRecord> {

    private CSVReader reader;

    public  CsvItemReader(Reader reader, CSVParser parser) {
        this.reader = new CSVReaderBuilder(reader)
                .withSkipLines(1)
                .withCSVParser(parser)
                .build();
    }

    @Override
    public CsvRecord read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        String[] content = reader.readNext();
        if (content == null) {
            reader.close();
            return  null;
        }
        return new CsvRecord(Arrays.asList(content));
    }
    
    public void close() {
    	try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
