package com.example.CSVtoFixed.Config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.FormatterLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import com.example.CSVtoFixed.Model.DataModel;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {
	
	@Autowired
	StepBuilderFactory stepbuilderfactory;

	@Autowired 
	JobBuilderFactory jobbuilderfactory;
	
	@Bean
	public Job FirstJob(FlatFileItemReader<DataModel> itemreader, FlatFileItemWriter<DataModel> itemwriter)
	{
		Step step=stepbuilderfactory.get("First_Step").
				<DataModel,DataModel>chunk(100).
				reader(itemreader).
				writer(itemwriter).
				build();
		
		
		return jobbuilderfactory.get("First_Job").
				incrementer(new RunIdIncrementer()).
				start(step).
				build();
		
	}
	
	@Bean
	public FlatFileItemReader<DataModel> itemreader()
	{
	
		return new FlatFileItemReaderBuilder<DataModel>().
				   name("CSV_reader").
				   resource(new ClassPathResource("CSV-Fixed.csv")).
			       linesToSkip(1).
			       delimited().
			       names(new String[]{"Id","First_Name","Last_Name","Email"}).
				   fieldSetMapper(new BeanWrapperFieldSetMapper<DataModel>(){{setTargetType(DataModel.class);}}).
				   build();
	}
	
	@Bean
	public FlatFileItemWriter<DataModel> itemwriter()
	{
		
		BeanWrapperFieldExtractor<DataModel> fieldExtractor = new BeanWrapperFieldExtractor<>();
		fieldExtractor.setNames(new String[] {"Id","First_Name","Last_Name","Email"});
		fieldExtractor.afterPropertiesSet();

		FormatterLineAggregator<DataModel> lineAggregator = new FormatterLineAggregator<>();
		lineAggregator.setFormat("%-8.8s%-10.10s%-10.10s%-25.25s");
		lineAggregator.setFieldExtractor(fieldExtractor);
		
		return new FlatFileItemWriterBuilder<DataModel>().
				   name("Fixed_Writer").
				   resource(new FileSystemResource("output/fixed.txt")).
				   lineAggregator(lineAggregator).
				   build();
	}
	

	
}
