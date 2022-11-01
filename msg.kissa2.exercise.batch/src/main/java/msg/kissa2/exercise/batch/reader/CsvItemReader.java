package msg.kissa2.exercise.batch.reader;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import msg.kissa2.exercise.batch.data.CsvRecord;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import java.io.Reader;
import java.util.Arrays;
import java.util.List;

public class CsvItemReader implements ItemReader<CsvRecord> {

    private CSVReader reader;

    public  CsvItemReader(Reader reader, CSVParser parser) {
        this.reader = new CSVReaderBuilder(reader)
                .withSkipLines(0)
                .withCSVParser(parser)
                .build();
    }

    @Override
    public CsvRecord read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return new CsvRecord(Arrays.asList(reader.readNext()));
    }
}
