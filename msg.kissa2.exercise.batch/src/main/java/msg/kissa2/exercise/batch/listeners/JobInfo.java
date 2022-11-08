package msg.kissa2.exercise.batch.listeners;

// singleton info holder bean

public class JobInfo {
	public Integer timpl100LinesTotal = 0;
	public Integer timpl101LinesTotal = 0;
	public Integer timpl102LinesTotal = 0;

	// number of filtered lines
	public Integer timpl100Lines = 0;
	public Integer timpl101Lines = 0;
	public Integer timpl102Lines = 0;

	// from this below measure conditions for timpl100 only
	public Integer filteredByCond_EFF_ZS = 0;
	public Integer filteredByCond_BEW_DAR_BETR = 0;
	public Integer filteredByCond_STATUS_FORWARD_KZ = 0;
	public Integer filteredByCond_dates = 0;
	public Integer filteredByCond_AUFL_KZ = 0;
	public Integer filteredByCond_STUFE = 0;
	public Integer filteredByCond_BETRAG_OPT = 0;
	public Integer filteredByCond_BETRAG_MANUELL_KZ = 0;

	private static JobInfo INSTANCE;
	
	private JobInfo() {
	}
	
	public static JobInfo getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new JobInfo();
		}
		
		return INSTANCE;
	}
	
}
