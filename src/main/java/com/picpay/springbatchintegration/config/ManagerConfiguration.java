package com.picpay.springbatchintegration.config;

import com.picpay.springbatchintegration.partitioner.BasicPartitioner;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;

/**
 * This configuration class is for the manager side of the remote partitioning sample. The
 * manager step will create 3 partitions for workers to process.
 *
 * @author Mahmoud Ben Hassine
 */
@Configuration
@EnableBatchProcessing
@EnableBatchIntegration
@Import(value = { DataSourceConfiguration.class, BrokerConfiguration.class })
public class ManagerConfiguration {

    private static final int GRID_SIZE = 3;

    private final RemotePartitioningManagerStepBuilderFactory managerStepBuilderFactory;

    public ManagerConfiguration(RemotePartitioningManagerStepBuilderFactory managerStepBuilderFactory) {

        this.managerStepBuilderFactory = managerStepBuilderFactory;
    }

    /*
     * Configure outbound flow (requests going to workers)
     */
    @Bean
    public DirectChannel requests() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow outboundFlow(ProducerFactory producerFactory) {
        return IntegrationFlow.from(requests())
                .handle(Kafka.outboundChannelAdapter(producerFactory).topic("requests"))
                .get();
    }


    /*
     * Configure inbound flow (replies coming from workers)
     */
    @Bean
    public DirectChannel replies() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow inboundFlow(ConsumerFactory consumerFactory) {
        return IntegrationFlow.from(
                        Kafka.messageDrivenChannelAdapter(consumerFactory,
                                KafkaMessageDrivenChannelAdapter.ListenerMode.record,"replies"))
                .channel(replies())
                .get();
    }

    /*
     * Configure the manager step
     */
    @Bean
    public Step managerStep() {
        return this.managerStepBuilderFactory.get("managerStep")
                .partitioner("workerStep", new BasicPartitioner())
                .gridSize(GRID_SIZE)
                .outputChannel(requests())
                .inputChannel(replies())
                .build();
    }

    @Bean
    public Job remotePartitioningJob(JobRepository jobRepository) {
        return new JobBuilder("remotePartitioningJob", jobRepository).start(managerStep()).build();
    }

}
