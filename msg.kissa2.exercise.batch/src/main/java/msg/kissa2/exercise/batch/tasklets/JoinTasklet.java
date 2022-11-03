package msg.kissa2.exercise.batch.tasklets;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

//basic idea:
	//	open connection to each files, and perform join in memory
	//  read from 100 & 101 => join => read from 102 => join with prev. reult => write result

public class JoinTasklet implements Tasklet {

	// TODO: implement execute method
	
	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		System.out.println("FIGYELEM: EZ ITTEN A JOIN TASKLET");
		return RepeatStatus.FINISHED;
	}

}
