package msg.kissa2.exercise.batch.listeners;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

public class FilterProcessListener implements StepExecutionListener {

	@Override
	public void beforeStep(StepExecution stepExecution) {

	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		System.out.println("Total lines:");
		System.out.println("\tTimpl100: " + JobInfo.getInstance().timpl100LinesTotal);
		System.out.println("\tTimpl101: " + JobInfo.getInstance().timpl101LinesTotal);
		System.out.println("\tTimpl102: " + JobInfo.getInstance().timpl102LinesTotal);

		System.out.println("Row count filtered by condition:");
		System.out.println("\tEFF_ZS > 0 :                        " + JobInfo.getInstance().filteredByCond_EFF_ZS);
		System.out.println("\tBEW_DAR_BETR > 0 :                  " + JobInfo.getInstance().filteredByCond_BEW_DAR_BETR);
		System.out.println("\tSTATUS == 2 OR FORWARD_KZ = 0 :     " + JobInfo.getInstance().filteredByCond_STATUS_FORWARD_KZ);
		System.out.println("\tSOTI_DATAB < VERTR_DATBI :          " + JobInfo.getInstance().filteredByCond_dates);
		System.out.println("\tAUFL_KZ != 2 :                      " + JobInfo.getInstance().filteredByCond_AUFL_KZ);
		System.out.println("\tSTUFE < 999 :                       " + JobInfo.getInstance().filteredByCond_STUFE);
		System.out.println("\tBETRAG_OPT > 0 :                    " + JobInfo.getInstance().filteredByCond_BETRAG_OPT);
		System.out.println("\tBETRAG > 10 AND MANUELL_KZ != ‘S’ : " + JobInfo.getInstance().filteredByCond_BETRAG_MANUELL_KZ);

		System.out.println("Filtered row count:");
		System.out.println("\tTimpl100: " + JobInfo.getInstance().timpl100Lines);
		System.out.println("\tTimpl101: " + JobInfo.getInstance().timpl101Lines);
		System.out.println("\tTimpl102: " + JobInfo.getInstance().timpl102Lines);

		System.out.println("=================================");
		return ExitStatus.COMPLETED;
	}

}
