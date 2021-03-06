package com.huc.batch.config;


import com.huc.batch.listener.MyJobListener;
import com.huc.batch.listener.MyReadListener;
import com.huc.batch.listener.MyWriteListener;
import com.huc.batch.pojo.BlogInfo;
import com.huc.batch.validate.MyBeanValidator;
import com.huc.batch.validate.MyItemProcessor;
import com.huc.batch.validate.MyItemProcessorNew;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.MyBatisCursorItemReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
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
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableBatchProcessing
public class MyBatchConfig {

    private Logger logger = LoggerFactory.getLogger(MyBatchConfig.class);


    /**
     * JobRepository?????????Job???????????????????????????????????????????????????????????????
     *
     * @param dataSource
     * @param transactionManager
     * @return
     * @throws Exception
     */
    @Bean
    public JobRepository myJobRepository(DataSource dataSource, PlatformTransactionManager transactionManager) throws Exception {
        JobRepositoryFactoryBean jobRepositoryFactoryBean = new JobRepositoryFactoryBean();
        jobRepositoryFactoryBean.setDatabaseType("mysql");
        jobRepositoryFactoryBean.setTransactionManager(transactionManager);
        jobRepositoryFactoryBean.setDataSource(dataSource);
        return jobRepositoryFactoryBean.getObject();
    }

    /**
     * jobLauncher????????? job????????????,???????????????jobRepository
     *
     * @param dataSource
     * @param transactionManager
     * @return
     * @throws Exception
     */
    @Bean
    public SimpleJobLauncher myJobLauncher(DataSource dataSource, PlatformTransactionManager transactionManager) throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        // ??????jobRepository
        jobLauncher.setJobRepository(myJobRepository(dataSource, transactionManager));
        return jobLauncher;
    }


    /**
     * ??????job
     *
     * @param jobs
     * @param myStep
     * @return
     */
    @Bean
    public Job myJob(JobBuilderFactory jobs, Step myStep) {
        return jobs.get("myJob")
                .incrementer(new RunIdIncrementer())
                .flow(myStep)
                .end()
                .listener(myJobListener())
                .build();
    }

    /**
     * ??????job?????????
     *
     * @return
     */
    @Bean
    public MyJobListener myJobListener() {
        return new MyJobListener();
    }


    @Bean
    public ItemReader<BlogInfo> reader() {
        // ??????FlatFileItemReader??????cvs??????????????????????????????
        FlatFileItemReader<BlogInfo> reader = new FlatFileItemReader<>();
        // ????????????????????????
        reader.setResource(new ClassPathResource("static/bloginfo.csv"));
        // entity???csv???????????????
        reader.setLineMapper(new DefaultLineMapper<BlogInfo>() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        //<editor-fold desc="csv ???????????????????????????????????????">
                        setNames(new String[]{"blogAuthor", "blogUrl", "blogTitle", "blogItem"});
                        //</editor-fold>

                    }
                });
                setFieldSetMapper(new BeanWrapperFieldSetMapper<BlogInfo>() {
                    {
                        setTargetType(BlogInfo.class);
                    }
                });
            }
        });
        return reader;
    }

    /**
     * ??????ItemProcessor: ????????????+????????????
     *
     * @return
     */
    @Bean
    public ItemProcessor<BlogInfo, BlogInfo> processor() {
        MyItemProcessor myItemProcessor = new MyItemProcessor();
        // ???????????????
        myItemProcessor.setValidator(myBeanValidator());
        return myItemProcessor;
    }


    /**
     * ???????????????
     *
     * @return
     */
    @Bean
    public MyBeanValidator myBeanValidator() {
        return new MyBeanValidator<BlogInfo>();
    }

    /**
     * ItemWriter???????????????datasource?????????????????????sql????????????????????????
     * @param dataSource
     * @return
     */
    @Bean
    public ItemWriter<BlogInfo> writer(DataSource dataSource){
        // ??????jdbcBcatchItemWrite????????????????????????
        JdbcBatchItemWriter<BlogInfo> writer = new JdbcBatchItemWriter<>();
        // ??????????????????sql??????
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<BlogInfo>());
        String sql = "insert into bloginfo "+" (blogAuthor,blogUrl,blogTitle,blogItem) "
                +" values(:blogAuthor,:blogUrl,:blogTitle,:blogItem)";
        writer.setSql(sql);
        writer.setDataSource(dataSource);
        return writer;
    }

    @Bean
    public Step myStep(StepBuilderFactory stepBuilderFactory, ItemReader<BlogInfo> reader,
                       ItemWriter<BlogInfo> writer, ItemProcessor<BlogInfo, BlogInfo> processor){
        return stepBuilderFactory
                .get("myStep")
                .<BlogInfo, BlogInfo>chunk(65000) // Chunk?????????(????????????????????????????????????????????????????????????????????????????????????????????????writer??????????????????)
                .reader(reader).faultTolerant().retryLimit(3).retry(Exception.class).skip(Exception.class).skipLimit(2)
                .listener(new MyReadListener())
                .processor(processor)
                .writer(writer).faultTolerant().skip(Exception.class).skipLimit(2)
                .listener(new MyWriteListener())
                .build();
    }


    /**
     * ??????job
     * @param jobs
     * @param stepNew
     * @return
     */
    @Bean
    public Job myJobNew(JobBuilderFactory jobs, Step stepNew){
        return jobs.get("myJobNew")
                .incrementer(new RunIdIncrementer())
                .flow(stepNew)
                .end()
                .listener(myJobListener())
                .build();

    }


    @Bean
    public Step stepNew(StepBuilderFactory stepBuilderFactory, MyBatisCursorItemReader<BlogInfo> itemReaderNew,
                        ItemWriter<BlogInfo> writerNew, ItemProcessor<BlogInfo, BlogInfo> processorNew){
        return stepBuilderFactory
                .get("stepNew")
                .<BlogInfo, BlogInfo>chunk(65000) // Chunk?????????(????????????????????????????????????????????????????????????????????????????????????????????????writer??????????????????)
                .reader(itemReaderNew).faultTolerant().retryLimit(3).retry(Exception.class).skip(Exception.class).skipLimit(10)
                .listener(new MyReadListener())
                .processor(processorNew)
                .writer(writerNew).faultTolerant().skip(Exception.class).skipLimit(2)
                .listener(new MyWriteListener())
                .build();

    }

    @Bean
    public ItemProcessor<BlogInfo, BlogInfo> processorNew(){
        MyItemProcessorNew csvItemProcessor = new MyItemProcessorNew();
        // ???????????????
        csvItemProcessor.setValidator(myBeanValidator());
        return csvItemProcessor;
    }



    @Autowired
    private SqlSessionFactory sqlSessionFactory;

    @Bean
    @StepScope//???????????????????????????@Value??????
    //Spring Batch????????????????????????bean scope??????StepScope:????????????????????????Spring bean scope????????????step scope??????????????????batches?????????steps??????????????????????????????Spring???beans???steps?????????????????????????????????????????????step????????????????????????
    public MyBatisCursorItemReader<BlogInfo> itemReaderNew(@Value("#{jobParameters[authorId]}") String authorId) {

        System.out.println("?????????????????????");

        MyBatisCursorItemReader<BlogInfo> reader = new MyBatisCursorItemReader<>();

        reader.setQueryId("com.huc.batch.mapper.BlogMapper.queryInfoById");

        reader.setSqlSessionFactory(sqlSessionFactory);
        Map<String , Object> map = new HashMap<>();

        map.put("authorId" , Integer.valueOf(authorId));//sql????????????
        reader.setParameterValues(map);
        return reader;
    }

    /**
     * ItemWriter???????????????datasource?????????????????????sql????????????????????????
     * @param dataSource
     * @return
     */
    @Bean
    public ItemWriter<BlogInfo> writerNew(DataSource dataSource){
        // ??????jdbcBcatchItemWrite????????????????????????
        JdbcBatchItemWriter<BlogInfo> writer = new JdbcBatchItemWriter<>();
        // ??????????????????sql??????
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<BlogInfo>());
        String sql = "insert into bloginfonew "+" (blogAuthor,blogUrl,blogTitle,blogItem) "
                +" values(:blogAuthor,:blogUrl,:blogTitle,:blogItem)";
        writer.setSql(sql);
        writer.setDataSource(dataSource);
        return writer;
    }


}
