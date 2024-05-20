package com.picpay.springbatchintegration.config;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * This configuration class is for the worker side of the remote partitioning sample. Each
 * worker will process a partition sent by the manager step.
 *
 * @author Mahmoud Ben Hassine
 */
@Configuration
@EnableBatchProcessing
@EnableBatchIntegration
@Import(value = { DataSourceConfiguration.class, BrokerConfiguration.class })
public class WorkerConfiguration {

    private final RemotePartitioningWorkerStepBuilderFactory workerStepBuilderFactory;

    public WorkerConfiguration(RemotePartitioningWorkerStepBuilderFactory workerStepBuilderFactory) {
        this.workerStepBuilderFactory = workerStepBuilderFactory;
    }


    @Bean
    public DirectChannel requests() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow outboundFlow(ProducerFactory producerFactory) {
        return IntegrationFlow.from(replies())
                .handle(Kafka.outboundChannelAdapter(producerFactory).topic("replies"))
                .get();
    }

    @Bean
    public DirectChannel replies() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow inboundFlow(ConsumerFactory consumerFactory) {
        return IntegrationFlow.from(
                        Kafka.messageDrivenChannelAdapter(consumerFactory,
                                KafkaMessageDrivenChannelAdapter.ListenerMode.record,"requests"))
                .channel(requests())
                .get();
    }

    @Bean
    public Step workerStep(PlatformTransactionManager transactionManager) {
        return this.workerStepBuilderFactory.get("workerStep")
                .inputChannel(requests())
                .outputChannel(replies())
                .tasklet(tasklet(null), transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet tasklet(@Value("#{stepExecutionContext['partition']}") String partition) {
        return (contribution, chunkContext) -> {
            System.out.println("processing " + partition);
            return RepeatStatus.FINISHED;
        };
    }

}