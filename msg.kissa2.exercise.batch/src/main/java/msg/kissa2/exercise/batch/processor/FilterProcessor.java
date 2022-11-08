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
import msg.kissa2.exercise.batch.listeners.JobInfo;

public class FilterProcessor implements ItemProcessor<CsvRecord, CsvRecord> {

	@Override
	public CsvRecord process(CsvRecord item) throws Exception {

		Table.Record record;

		String tableName = item.getList().get(0);

		// need to copy this CsvRecord obj., because convert works without table name
		// attr.
		// but the result must contain the table name (to keep it at writing)
		CsvRecord itemCopy = new CsvRecord(item);
		ArrayList<String> tmp = new ArrayList<String>(itemCopy.getList());
		tmp.remove(0);
		itemCopy.setList(tmp);

		if (tableName.equals("TIMPL100")) {
			JobInfo.getInstance().timpl100LinesTotal++;
			record = itemCopy.convertRecord(new Timpl100());
			if (isOk100(record)) {
				JobInfo.getInstance().timpl100Lines++;
				return item;
			}
		} else if (tableName.equals("TIMPL101")) {
			JobInfo.getInstance().timpl101LinesTotal++;
			record = itemCopy.convertRecord(new Timpl101());
			if (isOk101(record)) {
				JobInfo.getInstance().timpl101Lines++;
				return item;
			}
		} else if (tableName.equals("TIMPL102")) {
			JobInfo.getInstance().timpl102LinesTotal++;
			record = itemCopy.convertRecord(new Timpl102());
			if (isOk102(record)) {
				JobInfo.getInstance().timpl102Lines++;
				return item;
			}
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

			Boolean _eff_zs = eff_zs > 0;
			Boolean _bew_dar_betr = bew_dar_betr > 0;
			Boolean _status_forward_kz = status == 2 || forward_kz == 0;
			Boolean _dates = vertr_datbi.after(soti_datab);
			Boolean _aufl_kz = aufl_kz != 2;

			if (_eff_zs) {
				JobInfo.getInstance().filteredByCond_EFF_ZS++;
			}
			if (_bew_dar_betr) {
				JobInfo.getInstance().filteredByCond_BEW_DAR_BETR++;
			}
			if (_status_forward_kz) {
				JobInfo.getInstance().filteredByCond_STATUS_FORWARD_KZ++;
			}
			if (_dates) {
				JobInfo.getInstance().filteredByCond_dates++;
			}
			if (_aufl_kz) {
				JobInfo.getInstance().filteredByCond_AUFL_KZ++;
			}

			return _eff_zs && _bew_dar_betr && _status_forward_kz && _dates && _aufl_kz;
		} catch (Exception e) {
			return false;
		}
	}

	// conditions for filtering Timpl101 record
	private boolean isOk101(Table.Record record) {
		try {
			Double stufe = getNumericValue(record, "STUFE");
			Double betrag_opt = getNumericValue(record, "BETRAG_OPT");

			Boolean _stufe = stufe < 999;
			Boolean _betrag_opt = betrag_opt > 0;

			if (_stufe) {
				JobInfo.getInstance().filteredByCond_STUFE++;
			}
			if (_betrag_opt) {
				JobInfo.getInstance().filteredByCond_BETRAG_OPT++;
			}

			return _stufe && _betrag_opt;
		} catch (Exception e) {
			return false;
		}
	}

	// conditions for filtering Timpl102 record
	private boolean isOk102(Table.Record record) {
		try {
			Double betrag = getNumericValue(record, "BETRAG");
			String manuell_kz = record.getValue("MANUELL_KZ");

			if (betrag > 0 && !manuell_kz.equals("S")) {
				JobInfo.getInstance().filteredByCond_BETRAG_MANUELL_KZ++;
				return true;
			}
			return false;
		} catch (Exception e) {
			return false;
		}
	}
}
