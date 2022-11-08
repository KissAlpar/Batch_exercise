package msg.kissa2.exercise.batch.processor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.springframework.batch.item.ItemProcessor;

import msg.kissa2.exercise.batch.data.CsvRecord;
import msg.kissa2.exercise.batch.data.Table;
import msg.kissa2.exercise.batch.data.Timpl100;
import msg.kissa2.exercise.batch.data.Timpl101;
import msg.kissa2.exercise.batch.data.Timpl102;

public class FilterProcessor implements ItemProcessor<CsvRecord, CsvRecord> {

	@Override
	public CsvRecord process(CsvRecord item) throws Exception {

		Table.Record record;
		
		String tableName = item.getList().get(0);
		
		// need to copy this CsvRecord obj., because convert works without table name attr.
		// but the result must contain the table name (to keep it at writing)
		CsvRecord itemCopy = new CsvRecord(item);
		ArrayList<String> tmp = new ArrayList<String>(itemCopy.getList());
		tmp.remove(0);
		itemCopy.setList(tmp);
		
		if (tableName.equals("TIMPL100")) {
			record = itemCopy.convertRecord(new Timpl100());
			return isOk100(record) ? item : null;
		} else if (tableName.equals("TIMPL101")) {
			record = itemCopy.convertRecord(new Timpl101());
			return isOk101(record) ? item : null;
		} else if (tableName.equals("TIMPL102")) {
			record = itemCopy.convertRecord(new Timpl102());
			return isOk102(record) ? item : null;
		}

		return null;
	}

	// --- get typed / special formated attributes ---
	private Double getNumericValue(Table.Record record, String attributeName) throws Exception {
		return Double.parseDouble(record.getValue(attributeName));
	}

	private Date getDateValue(Table.Record record, String attributeName) throws Exception {
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd.mm.yyyy");
		try {
			return dateFormat.parse(record.getValue(attributeName));
		} catch (ParseException e) {
			throw new RuntimeException("Cannot convert record to date!");
		}
	}

	// --- filter conditions ---

	// conditions for filtering Timpl100 record
	private boolean isOk100(Table.Record record) {
		try {
			Double eff_zs = getNumericValue(record, "EFF_ZS");
			Double bew_dar_betr = getNumericValue(record, "BEW_DAR_BETR");
			Double status = getNumericValue(record, "STATUS");
			Double forward_kz = getNumericValue(record, "FORWARD_KZ");
			Date soti_datab = getDateValue(record, "SOTI_DATAB");
			Date vertr_datbi = getDateValue(record, "VERTR_DATBI");
			Double aufl_kz = getNumericValue(record, "AUFL_KZ");

			return (
					eff_zs > 0 
				&& bew_dar_betr > 0 
				&& (status == 2 || forward_kz == 0) 
				&& vertr_datbi.after(soti_datab)
				&& aufl_kz != 2
			);
		} catch (Exception e) {
			return false;
		}
	}

	// contitions for filtering Timpl101 record
	private boolean isOk101(Table.Record record) {
		try {
			Double stufe = getNumericValue(record, "STUFE");
			Double betrag_opt = getNumericValue(record, "BETRAG_OPT");

			return (stufe < 999 && betrag_opt > 0);
		} catch (Exception e) {
			return false;
		}
	}

	// contitions for filtering Timpl102 record
	private boolean isOk102(Table.Record record) {
		try {
			Double betrag = getNumericValue(record, "BETRAG");
			String manuell_kz = record.getValue("MANUELL_KZ");
	
			return (betrag > 0 && !manuell_kz.equals("S"));
		} catch (Exception e) {
			return false;
		}
	}
}
