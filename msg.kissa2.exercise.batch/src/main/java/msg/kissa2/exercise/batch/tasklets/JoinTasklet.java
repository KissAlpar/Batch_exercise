package msg.kissa2.exercise.batch.tasklets;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.repeat.RepeatStatus;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

import msg.kissa2.exercise.batch.data.CsvRecord;
import msg.kissa2.exercise.batch.data.JoinResultTable;
import msg.kissa2.exercise.batch.data.Table;
import msg.kissa2.exercise.batch.data.Timpl100;
import msg.kissa2.exercise.batch.data.Timpl101;
import msg.kissa2.exercise.batch.data.Timpl102;
import msg.kissa2.exercise.batch.reader.CsvItemReader;

// basic idea:
//	open connection to each files, and perform join in memory on chunks; write the output immediately 
//  and clear buffers to keep the memory usage on average / avoid high memory usage
//  (chunks size depends on the number of equal konto~p.k. records)
//  read from 100 & 101 => join => read from 102 => join with prev. reult => write result

public class JoinTasklet implements Tasklet {

	private CsvItemReader reader100;
	private CsvItemReader reader101;
	private CsvItemReader reader102;
	private CSVPrinter outputWriter;

	public JoinTasklet() throws IOException {
		CSVParser parser = new CSVParserBuilder().withSeparator(';').build();

		reader100 = new CsvItemReader(Files.newBufferedReader(Paths.get("./TIMPL100.csv")), parser);
		reader101 = new CsvItemReader(Files.newBufferedReader(Paths.get("./TIMPL101.csv")), parser);
		reader102 = new CsvItemReader(Files.newBufferedReader(Paths.get("./TIMPL102.csv")), parser);

		outputWriter = new CSVPrinter(new FileWriter("JOIN.csv"), CSVFormat.DEFAULT);
		outputWriter.printRecord(new JoinResultTable().getSchema().toArray());
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		List<Table.Record> fstTwoJoined = new ArrayList<>();
		List<Table.Record> finalResult = new ArrayList<>();

		// --- join timpl100 & timpl101 ---
		List<CsvRecord> records100 = new ArrayList<>();
		List<CsvRecord> records101 = new ArrayList<>();
		List<CsvRecord> records102 = new ArrayList<>();

		CsvRecord fst100 = readNextRecord(reader100);
		CsvRecord fst101 = readNextRecord(reader101);
		CsvRecord fst102 = readNextRecord(reader102);

		// while both files contains data
		while (fst100 != null && fst101 != null && fst102 != null) {

			// clear buffers, because their content is already joined
			records100.clear();
			records101.clear();
			records102.clear();

			Double konto100 = Double.parseDouble(fst100.getList().get(0));
			Double konto101 = Double.parseDouble(fst101.getList().get(0));

			// match konto field (because both files are ordered by this)
			CsvRecord rec100 = fst100, rec101 = fst101, rec102 = fst102;
			while (konto100.compareTo(konto101) < 0) {
				if ((rec100 = readNextRecord(reader100)) == null) {
					break;
				}
				konto100 = Double.parseDouble(rec100.getList().get(0));
			}

			while (konto100.compareTo(konto101) > 0) {
				if ((rec101 = readNextRecord(reader101)) == null) {
					break;
				}
				konto101 = Double.parseDouble(rec101.getList().get(0));
			}

			// save records with matching konto
			while (konto100.compareTo(konto101) == 0) {
				records100.add(rec100);
				records101.add(rec101);
				if ((rec100 = readNextRecord(reader100)) == null) {
					break;
				}
				if ((rec101 = readNextRecord(reader101)) == null) {
					break;
				}
				konto100 = Double.parseDouble(rec100.getList().get(0));
				konto101 = Double.parseDouble(rec101.getList().get(0));
			}

			// matching next konto
			// e.g. 101 contains more records with previous konto it needs to be added
			while (konto100.compareTo(konto101) < 0) {
				records100.add(rec100);
				if ((rec100 = readNextRecord(reader100)) == null) {
					break;
				}
				konto100 = Double.parseDouble(rec100.getList().get(0));
			}

			while (konto100.compareTo(konto101) > 0) {
				records101.add(rec101);
				if ((rec101 = readNextRecord(reader101)) == null) {
					break;
				}
				konto101 = Double.parseDouble(rec101.getList().get(0));
			}

			fstTwoJoined.addAll(innerJoin(records100, records101));

			// read until find matching values from last file (repeat steps)
			Double konto102 = Double.parseDouble(rec102.getList().get(1));
			konto100 = Double.parseDouble(fst100.getList().get(0));

			if (konto102.compareTo(konto100) > 0) {
				fst100 = rec100;
				fst101 = rec101;
				continue;
			}

			while (konto102.compareTo(konto100) < 0) {
				if ((rec102 = readNextRecord(reader102)) == null) {
					break;
				} else {
					konto102 = Double.parseDouble(rec102.getList().get(1));
				}
			}

			while (konto102.compareTo(konto100) == 0) {
				records102.add(rec102);
				if ((rec102 = readNextRecord(reader102)) == null) {
					break;
				} else {
					try {
						konto102 = Double.parseDouble(rec102.getList().get(1));
					} catch (IndexOutOfBoundsException e) {
						// empty line at the end of the file
						// do nothing, error is handled in other functions
					}
				}
			}

			finalResult.addAll(leftJoin(fstTwoJoined, records102));

			// write final result chunk to output file
			writeOutput(finalResult);
			fstTwoJoined.clear();
			finalResult.clear();

			fst100 = rec100;
			fst101 = rec101;
			fst102 = rec102;
		}

		outputWriter.close();
		return RepeatStatus.FINISHED;
	}

	private void closeConnections() {
		reader100.close();
		reader101.close();
		reader102.close();
	}

	private void writeOutput(List<Table.Record> records) throws IOException {
		for (int i = 0; i < records.size(); i++) {
			// put records manually to keep correct order
			ArrayList<String> data = new ArrayList<>();
			data.add(records.get(i).getValue("KONTO"));
			data.add(records.get(i).getValue("STATUS"));
			data.add(records.get(i).getValue("STUFE"));
			data.add(records.get(i).getValue("KTOSALDO"));
			data.add(records.get(i).getValue("VERTR_DATAB"));
			data.add(records.get(i).getValue("VERTR_DATBI"));
			data.add(records.get(i).getValue("BEW_DAR_BETR"));
			data.add(records.get(i).getValue("ZS"));
			data.add(records.get(i).getValue("EFF_ZS"));
			data.add(records.get(i).getValue("FORWARD_KZ"));
			data.add(records.get(i).getValue("AUFL_KZ"));
			data.add(records.get(i).getValue("BGB_KZ"));
			data.add(records.get(i).getValue("SOTI_DATAB"));
			data.add(records.get(i).getValue("SOTI_DATBI"));
			data.add(records.get(i).getValue("PRODUKTNR"));
			data.add(records.get(i).getValue("ANL_DAT"));
			data.add(records.get(i).getValue("DATUM_VON"));
			data.add(records.get(i).getValue("DATUM_BIS"));
			data.add(records.get(i).getValue("BETRAG_OPT"));
			data.add(records.get(i).getValue("BETRAG_SOTI"));
			data.add(records.get(i).getValue("DATUM"));
			data.add(records.get(i).getValue("BETRAG"));
			data.add(records.get(i).getValue("TYP"));
			outputWriter.printRecord(data);
		}
	}

	// join on konto & status & stufe
	private List<Table.Record> leftJoin(List<Table.Record> leftSideRecords, List<CsvRecord> redords3) {
		// get right side records with schema
		List<Table.Record> rightSideRecords = new ArrayList<>();
		for (CsvRecord rec : redords3) {
			Table.Record convRec = rec.convertRecord(new Timpl102());
			rightSideRecords.add(convRec);
		}

		// perform left join
		List<Table.Record> joinRes = new ArrayList<>();
		for (Table.Record rec1 : leftSideRecords) {
			for (Table.Record rec2 : rightSideRecords) {
				if (rec1 == null || rec2 == null) {
					continue;
				}

				String status1 = rec1.getValue("STATUS");
				String status2 = rec2.getValue("STATUS");
				String stufe1 = rec1.getValue("STUFE");
				String stufe2 = rec2.getValue("STUFE");
				if (status1.equals(status2) && stufe1.equals(stufe2)) {
					Table.Record joinedRec = joinRecords(rec1, rec2, new JoinResultTable());
					if (joinedRec == null) {
						continue;
					}
					joinRes.add(joinedRec);
				} else {
					joinRes.add(rec1); // adding left side values
				}
			}
		}

		return joinRes;
	}

	// join on konto & status
	private List<Table.Record> innerJoin(List<CsvRecord> records1, List<CsvRecord> records2) {

		// conversion to records (with known schema)
		List<Table.Record> leftSideRecords = new ArrayList<>();
		for (CsvRecord rec : records1) {
			Table.Record convRec = rec.convertRecord(new Timpl100());
			leftSideRecords.add(convRec);
		}
		List<Table.Record> rightSideRecords = new ArrayList<>();
		for (CsvRecord rec : records2) {
			Table.Record convRec = rec.convertRecord(new Timpl101());
			rightSideRecords.add(convRec);
		}

		// perform inner join
		List<Table.Record> fstTwoJoined = new ArrayList<>();
		for (Table.Record rec1 : leftSideRecords) {
			for (Table.Record rec2 : rightSideRecords) {
				if (rec1 == null || rec2 == null) {
					continue;
				}

				String status1 = rec1.getValue("STATUS");
				String status2 = rec2.getValue("STATUS");
				if (status1.equals(status2)) {
					Table.Record joinedRec = joinRecords(rec1, rec2, new JoinResultTable());
					if (joinedRec == null) { // if the join cannot happen (e.g. because some fields doesn't match the
												// schema)
						continue;
					}
					fstTwoJoined.add(joinedRec);
				}
			}
		}
		return fstTwoJoined;
	}

	private Table.Record joinRecords(Table.Record rec1, Table.Record rec2, Table table) {
		if (rec1 == null || rec2 == null) {
			return null;
		}

		Table.Record result = new Table.Record();

		for (String attributeName : table.getSchema()) {
			String res1 = rec1.getValue(attributeName);
			if (res1 != null) {
				result.setAttribute(attributeName, res1);
				continue;
			}

			String res2 = rec2.getValue(attributeName);
			result.setAttribute(attributeName, res2); // may set null here (missing fields from 3. join)
		}

		return result;
	}

	private CsvRecord readNextRecord(CsvItemReader reader) {
		try {
			CsvRecord res = reader.read(); // 1 line => need to split by comma
			if (res == null) {
				closeConnections();
				return null;
			}

			String[] splitted = res.getList().get(0).split(",", -1);
			res.setList(Arrays.asList(splitted));

			return res;
		} catch (UnexpectedInputException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (NonTransientResourceException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
