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
     * JobRepository定义：Job的注册容器以及和数据库打交道（事务管理等）
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
     * jobLauncher定义： job的启动器,绑定相关的jobRepository
     *
     * @param dataSource
     * @param transactionManager
     * @return
     * @throws Exception
     */
    @Bean
    public SimpleJobLauncher myJobLauncher(DataSource dataSource, PlatformTransactionManager transactionManager) throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        // 设置jobRepository
        jobLauncher.setJobRepository(myJobRepository(dataSource, transactionManager));
        return jobLauncher;
    }


    /**
     * 定义job
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
     * 注册job监听器
     *
     * @return
     */
    @Bean
    public MyJobListener myJobListener() {
        return new MyJobListener();
    }


    @Bean
    public ItemReader<BlogInfo> reader() {
        // 使用FlatFileItemReader去读cvs文件，一行即一条数据
        FlatFileItemReader<BlogInfo> reader = new FlatFileItemReader<>();
        // 设置文件处在路径
        reader.setResource(new ClassPathResource("static/bloginfo.csv"));
        // entity与csv数据做映射
        reader.setLineMapper(new DefaultLineMapper<BlogInfo>() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        //<editor-fold desc="csv 文件里各列按顺序对应的字段">
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
     * 注册ItemProcessor: 处理数据+校验数据
     *
     * @return
     */
    @Bean
    public ItemProcessor<BlogInfo, BlogInfo> processor() {
        MyItemProcessor myItemProcessor = new MyItemProcessor();
        // 设置校验器
        myItemProcessor.setValidator(myBeanValidator());
        return myItemProcessor;
    }


    /**
     * 注册校验器
     *
     * @return
     */
    @Bean
    public MyBeanValidator myBeanValidator() {
        return new MyBeanValidator<BlogInfo>();
    }

    /**
     * ItemWriter定义：指定datasource，设置批量插入sql语句，写入数据库
     * @param dataSource
     * @return
     */
    @Bean
    public ItemWriter<BlogInfo> writer(DataSource dataSource){
        // 使用jdbcBcatchItemWrite写数据到数据库中
        JdbcBatchItemWriter<BlogInfo> writer = new JdbcBatchItemWriter<>();
        // 设置有参数的sql语句
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
                .<BlogInfo, BlogInfo>chunk(65000) // Chunk的机制(即每次读取一条数据，再处理一条数据，累积到一定数量后再一次性交给writer进行写入操作)
                .reader(reader).faultTolerant().retryLimit(3).retry(Exception.class).skip(Exception.class).skipLimit(2)
                .listener(new MyReadListener())
                .processor(processor)
                .writer(writer).faultTolerant().skip(Exception.class).skipLimit(2)
                .listener(new MyWriteListener())
                .build();
    }


    /**
     * 定义job
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
                .<BlogInfo, BlogInfo>chunk(65000) // Chunk的机制(即每次读取一条数据，再处理一条数据，累积到一定数量后再一次性交给writer进行写入操作)
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
        // 设置校验器
        csvItemProcessor.setValidator(myBeanValidator());
        return csvItemProcessor;
    }



    @Autowired
    private SqlSessionFactory sqlSessionFactory;

    @Bean
    @StepScope//初始化的时候不检测@Value的值
    //Spring Batch提供了一个特殊的bean scope类（StepScope:作为一个自定义的Spring bean scope）。这个step scope的作用是连接batches的各个steps。这个机制允许配置在Spring的beans当steps开始时才实例化并且允许你为这个step指定配置和参数。
    public MyBatisCursorItemReader<BlogInfo> itemReaderNew(@Value("#{jobParameters[authorId]}") String authorId) {

        System.out.println("开始查询数据库");

        MyBatisCursorItemReader<BlogInfo> reader = new MyBatisCursorItemReader<>();

        reader.setQueryId("com.huc.batch.mapper.BlogMapper.queryInfoById");

        reader.setSqlSessionFactory(sqlSessionFactory);
        Map<String , Object> map = new HashMap<>();

        map.put("authorId" , Integer.valueOf(authorId));//sql查询参数
        reader.setParameterValues(map);
        return reader;
    }

    /**
     * ItemWriter定义：指定datasource，设置批量插入sql语句，写入数据库
     * @param dataSource
     * @return
     */
    @Bean
    public ItemWriter<BlogInfo> writerNew(DataSource dataSource){
        // 使用jdbcBcatchItemWrite写数据到数据库中
        JdbcBatchItemWriter<BlogInfo> writer = new JdbcBatchItemWriter<>();
        // 设置有参数的sql语句
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<BlogInfo>());
        String sql = "insert into bloginfonew "+" (blogAuthor,blogUrl,blogTitle,blogItem) "
                +" values(:blogAuthor,:blogUrl,:blogTitle,:blogItem)";
        writer.setSql(sql);
        writer.setDataSource(dataSource);
        return writer;
    }


}
