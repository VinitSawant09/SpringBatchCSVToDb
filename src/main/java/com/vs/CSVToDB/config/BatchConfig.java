package com.vs.CSVToDB.config;

import com.vs.CSVToDB.model.Product;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.PathResource;
import org.springframework.transaction.PlatformTransactionManager;
import javax.sql.DataSource;
import java.net.MalformedURLException;

@Configuration
public class BatchConfig {

    @Autowired
    DataSource dataSource;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Value("C:/Users/Admin/Desktop/GIG/SpringBatch/CSVToDB/src/main/java/products.csv")
    private String filePath;

    @Bean
    public ItemReader<Product> reader() throws MalformedURLException {
        FlatFileItemReader<Product> reader = new FlatFileItemReader<>();
        reader.setResource(new PathResource(filePath));

        DefaultLineMapper<Product> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames("id","names","description","price");
        BeanWrapperFieldSetMapper<Product> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Product.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        reader.setLineMapper(lineMapper);
        return reader;
    }

    @Bean
    public ItemProcessor<Product,Product> processor(){
        return (p) -> {
                p.setPrice(p.getPrice()-p.getPrice()*0.1);
                return p;
        };
    }

    @Bean
    public ItemWriter<Product> writer() {
        JdbcBatchItemWriter<Product> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Product>());
        writer.setSql("INSERT INTO PRODUCT(ID,NAME,DESCRIPTION,PRICE) VALUES (:id,:name,:description,:price)");
        return writer;
    }

    @Bean
    public Job job() throws MalformedURLException {
        return new JobBuilder("job-1",jobRepository).flow(step()).end().build();
    }

    @Bean
    public Step step() throws MalformedURLException {
        StepBuilder stepBuilder = new StepBuilder("step-1",jobRepository);
        return stepBuilder.<Product, Product>chunk(4, transactionManager).reader(reader()).processor(processor())
                .writer(writer()).build();

    }
}
