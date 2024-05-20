package com.picpay.springbatchintegration;

import com.picpay.springbatchintegration.config.ManagerConfiguration;
import com.picpay.springbatchintegration.config.RemotePartitioningJobConfiguration;
import com.picpay.springbatchintegration.config.WorkerConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
@ActiveProfiles("test")
@SpringJUnitConfig(classes = {ManagerConfiguration.class})
public class RemotePartitioningJobFunctionalTests {

    @Autowired
    protected JobLauncher jobLauncher;

    protected AnnotationConfigApplicationContext workerApplicationContext;

    @BeforeEach
    void setUp() {
        this.workerApplicationContext = new AnnotationConfigApplicationContext(WorkerConfiguration.class);
    }

    @Test
    void testRemotePartitioningJob(@Autowired Job job) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        // when
        JobExecution jobExecution = this.jobLauncher.run(job, new JobParameters());

        // then
        assertEquals(ExitStatus.COMPLETED.getExitCode(), jobExecution.getExitStatus().getExitCode());
        assertEquals(4, jobExecution.getStepExecutions().size()); // manager + 3
        // workers
    }
}
