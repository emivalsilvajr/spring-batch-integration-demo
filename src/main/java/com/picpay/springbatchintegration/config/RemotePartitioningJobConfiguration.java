package com.picpay.springbatchintegration.config;

import com.picpay.springbatchintegration.partitioner.BasicPartitioner;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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

@Configuration
@EnableBatchProcessing(dataSourceRef = "batchDataSource",transactionManagerRef = "batchTransactionManager")
@EnableBatchIntegration
@Import(value = { DataSourceConfiguration.class, BrokerConfiguration.class })
public class RemotePartitioningJobConfiguration {
    @Configuration
    public static class ManagerConfiguration {

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
        public IntegrationFlow outboundFlow(ConsumerFactory consumerFactory) {
            return IntegrationFlow.from(
                            Kafka.messageDrivenChannelAdapter(consumerFactory,
                                    KafkaMessageDrivenChannelAdapter.ListenerMode.record,"requests"))
                    .channel(requests())
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


        // Middleware beans setup omitted

    }

}
