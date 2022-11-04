package msg.kissa2.exercise.batch.data;

import java.util.ArrayList;
import java.util.Arrays;

public class JoinResultTable extends Table {

	@Override
	protected void buildStructure() {
		setName("JoinResult");
		schema = Arrays.asList(
				"KONTO",
				"STATUS",
				"STUFE",
				"KTOSALDO",
				"VERTR_DATAB",
				"VERTR_DATBI",
				"BEW_DAR_BETR",
				"ZS", 
				"EFF_ZS",
				"FORWARD_KZ",
				"AUFL_KZ", 
				"BGB_KZ",
				"SOTI_DATAB",
				"SOTI_DATBI",
				"PRODUKTNR",
				"ANL_DAT",
				"DATUM_VON", 	// Timpl101
				"DATUM_BIS", 	// Timpl101
				"BETRAG_OPT",	// Timpl101
				"BETRAG_SOTI",  // Timpl101
				"DATUM", 		// Timpl102
				"BETRAG",		// Timpl102
				"TYP"			// Timpl102
		);
		records = new ArrayList<>();
	}

}
