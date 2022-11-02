package msg.kissa2.exercise.batch.writer;

import java.beans.Statement;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PreDestroy;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.batch.item.ItemWriter;

import com.opencsv.CSVWriter;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;

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

	/*private ColumnPositionMappingStrategy<Statement> strategy1;
	private ColumnPositionMappingStrategy<Statement> strategy2;
	private ColumnPositionMappingStrategy<Statement> strategy3;

	private BufferedWriter writer1;
	private BufferedWriter writer2;
	private BufferedWriter writer3;
	private StatefulBeanToCsv<Statement> beanToCsv1;
	private StatefulBeanToCsv<Statement> beanToCsv2;
	private StatefulBeanToCsv<Statement> beanToCsv3;

	public DocumentWriter() throws Exception {

		
		strategy1 = new ColumnPositionMappingStrategy<Statement>();
		strategy1.setType(Statement.class);
		strategy1.setColumnMapping(
				new Timpl100().getSchema().toArray(new String[40])
				);
		
		strategy2 = new ColumnPositionMappingStrategy<Statement>();
		strategy2.setType(Statement.class);
		strategy2.setColumnMapping(
				new Timpl101().getSchema().toArray(new String[40])
				);

		strategy3 = new ColumnPositionMappingStrategy<Statement>();
		strategy3.setType(Statement.class);
		strategy3.setColumnMapping(
				new Timpl102().getSchema().toArray(new String[40])
				);


		File cdf1 = new File("TIMPL100.csv");
		File cdf2 = new File("TIMPL101.csv");
		File cdf3 = new File("TIMPL102.csv");

		if (cdf1.exists()) {
			writer1 = Files.newBufferedWriter(Paths.get( "TIMPL100.csv"), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
		} else {
			writer1 = Files.newBufferedWriter(Paths.get( "TIMPL100.csv"), StandardCharsets.UTF_8,
					StandardOpenOption.CREATE_NEW);
		}
		
		if (cdf2.exists()) {
			writer2 = Files.newBufferedWriter(Paths.get( "TIMPL101.csv"), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
		} else {
			writer2 = Files.newBufferedWriter(Paths.get( "TIMPL101.csv"), StandardCharsets.UTF_8,
					StandardOpenOption.CREATE_NEW);
		}
		
		if (cdf3.exists()) {
			writer3 = Files.newBufferedWriter(Paths.get( "TIMPL102.csv"), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
		} else {
			writer3 = Files.newBufferedWriter(Paths.get( "TIMPL102.csv"), StandardCharsets.UTF_8,
					StandardOpenOption.CREATE_NEW);
		}

		beanToCsv1 = new StatefulBeanToCsvBuilder<Statement>(writer1).withQuotechar(CSVWriter.NO_QUOTE_CHARACTER)
				.withMappingStrategy(strategy1).withSeparator(';').build();
		beanToCsv2 = new StatefulBeanToCsvBuilder<Statement>(writer2).withQuotechar(CSVWriter.NO_QUOTE_CHARACTER)
				.withMappingStrategy(strategy2).withSeparator(';').build();
		beanToCsv3 = new StatefulBeanToCsvBuilder<Statement>(writer3).withQuotechar(CSVWriter.NO_QUOTE_CHARACTER)
				.withMappingStrategy(strategy3).withSeparator(';').build();
	}

	@Override
	public void write(List<? extends CsvRecord> items) throws Exception {
		List<Statement> settlementList1 = new ArrayList<Statement>();
		List<Statement> settlementList2 = new ArrayList<Statement>();
		List<Statement> settlementList3 = new ArrayList<Statement>();

		for (int i = 0; i < items.size(); i++) {
			CsvRecord rec = items.get(i);
			if (rec.getList().get(0).equals("TIMPL100")) {
				for (String v : rec.getList()) {
					settlementList1.add(new Statement(v, null, null));
				}
			} else if (rec.getList().get(0).equals("TIMPL101")) {
				for (String v : rec.getList()) {
					settlementList2.add(new Statement(v, null, null));
				}
			} else {
				for (String v : rec.getList()) {
					settlementList3.add(new Statement(v, null, null));
				}
			}
		}

		beanToCsv1.write(settlementList1);
		beanToCsv2.write(settlementList1);
		beanToCsv3.write(settlementList1);

		writer1.flush();
		writer2.flush();
		writer3.flush();
	}

	@PreDestroy
	@Override
	public void close() throws IOException {
		writer1.close();
		writer2.close();
		writer3.close();
	}*/
}