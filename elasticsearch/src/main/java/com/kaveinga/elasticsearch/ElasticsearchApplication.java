package com.kaveinga.elasticsearch;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
public class ElasticsearchApplication {

    public static void main(String[] args) {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SpringApplication.run(ElasticsearchApplication.class, args);
    }

    @Autowired
    @Qualifier(value = "taskExecutor")
    private ThreadPoolTaskExecutor taskExecutor;

    @Autowired
    private HikariDataSource       hikariDataSource;

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {

            // Display Environmental Useful Variables
            try {
                System.out.println("\n");
                Runtime runtime = Runtime.getRuntime();
                double mb = 1048576;// megabtye to byte
                double gb = 1073741824;// gigabyte to byte
                Environment env = ctx.getEnvironment();
                TimeZone timeZone = TimeZone.getDefault();

                System.out.println("************************ Elasticsearch ***************************");
                System.out.println("** Active Profile: " + Arrays.toString(env.getActiveProfiles()));
                System.out.println("** Port: " + env.getProperty("server.port"));
                System.out.println("** Timezone: " + timeZone.getID());
                System.out.println("** TimeStamp: " + new Date());

                System.out.println("** Internal Url: http://" + env.getProperty("project.host") + ":" + env.getProperty("server.port") + env.getProperty("server.servlet.context-path"));
                System.out.println("** External Url: http://" + InetAddress.getLocalHost().getHostAddress() + ":" + env.getProperty("server.port") + env.getProperty("server.servlet.context-path"));

                System.out.println(
                        "** Internal Swagger: http://" + env.getProperty("project.host") + ":" + env.getProperty("server.port") + env.getProperty("server.servlet.context-path") + "/swagger-ui.html");
                System.out.println("** External Swagger: http://" + InetAddress.getLocalHost().getHostAddress() + ":" + env.getProperty("server.port") + env.getProperty("server.servlet.context-path")
                        + "/swagger-ui.html");

                System.out.println("************************* Database - Config **********************************");
                System.out.println("** Pool Size: " + hikariDataSource.getMaximumPoolSize());
                System.out.println("** Minimum Idle Connections: " + hikariDataSource.getMinimumIdle());
                System.out.println("** Connection Timeout: " + hikariDataSource.getConnectionTimeout() / 1000 + " seconds");
                System.out.println("** Connection Idle Timeout: " + (hikariDataSource.getIdleTimeout() / 1000) + " seconds");
                System.out.println("** Maximum Life Time: " + hikariDataSource.getMaxLifetime() / 1000 + " seconds");

                System.out.println("************************* Java - JVM *****************************************");
                System.out.println("** Number of processors: " + runtime.availableProcessors());
                String processName = ManagementFactory.getRuntimeMXBean().getName();
                System.out.println("** Process ID: " + processName.split("@")[0]);
                System.out.println("** Total memory: " + (runtime.totalMemory() / mb) + " MB = " + (runtime.totalMemory() / gb) + " GB");
                System.out.println("** Max memory: " + (runtime.maxMemory() / mb) + " MB = " + (runtime.maxMemory() / gb) + " GB");
                System.out.println("** Free memory: " + (runtime.freeMemory() / mb) + " MB = " + (runtime.freeMemory() / gb) + " GB");

                System.out.println("************************* Thread Pool ******************************************");
                System.out.println("** Thread Pool Core Size: " + taskExecutor.getCorePoolSize());
                System.out.println("** Thread Pool Active Count: " + taskExecutor.getActiveCount());
                System.out.println("** Thread Pool Max Size: " + taskExecutor.getMaxPoolSize());
                System.out.println("** Thread Pool Keep Alive Secs: " + taskExecutor.getKeepAliveSeconds());
                System.out.println("** Thread Pool Prefix: " + taskExecutor.getThreadNamePrefix());
                System.out.println("** Thread Pool Priority: " + taskExecutor.getThreadPriority());

                System.out.println("********************************************************************************");

            } catch (Exception e) {
                log.error("Exception, commandlineRunner -> {}", e.getMessage(), e);
            }
            System.out.println("\n");
        };
    }

    // @Autowired
    // private PropertyDAO propertyDAO;
    //
    // @Autowired
    // private PropertyES propertyES;
    //
    // @Autowired
    // private ElasticLoaderService elasticLoaderService;
    //
    // @Value("${spring.datasource.url}")
    // private String databaseHost;
    //
    // @Value("${elasticsearch.host}")
    // private String elasticsearchHost;
    //
    // @Value("${spring.datasource.database}")
    // private String database;
    //
    // @Value("${elasticsearch.create.mapping}")
    // private boolean createMapping;
    //
    // @Value("${elasticsearch.load.data}")
    // private boolean loadData;
    //
    // @Value("${elasticsearch.load.size:20000}")
    // private int loadSize;
    //
    // @Value("${elasticsearch.load.piece.size:4000}")
    // private int loadPieceSize;
    //
    // @Value("${elasticsearch.load.start.at:0}")
    // private int loadStartAt;
    //
    // @Value("${turnoffserver.after.run}")
    // private boolean turnOffServerAfterRun;
    //
    // @Autowired
    // private ElasticMappingService elasticMappingService;
    //
    // public void run(ApplicationContext ctx) {
    // log.info("loading data from database='{}' to elasticsearch='{}'", databaseHost, elasticsearchHost);
    // log.info("database name='{}', elasticsearch index='{}'", database, database.toLowerCase());
    //
    // log.info("createMapping={},loadData={}", createMapping, loadData);
    //
    // if (createMapping) {
    // elasticMappingService.setupMapping();
    // }
    //
    // if (loadData) {
    //
    // long startTime = System.currentTimeMillis();
    //
    // Long biggestPropertyMasterKey = propertyDAO.getBiggestPropertyId();
    //
    // Long dbRowCount = propertyDAO.getRowCount();
    //
    // log.info("dbRowCount={}, biggestPropertyMasterKey={}", dbRowCount, biggestPropertyMasterKey);
    //
    // int size = loadSize;
    //
    // int piece = loadPieceSize;
    //
    // Long count = 0L;
    //
    // int startAt = loadStartAt;
    //
    // for (int start = startAt; start < biggestPropertyMasterKey; start += (size)) {
    //
    // int end = (start + size);
    //
    // // log.info("start={}, end={}", start, end);
    //
    // List<CompletableFuture<Long>> loads = new ArrayList<>();
    //
    // // 5 batches at a time for total of size
    // for (int newStart = start, newEnd = (start + piece); newStart < end; newStart += piece, newEnd += piece) {
    //
    // // log.info("loading start={}, end={}", start, end);
    //
    // loads.add(elasticLoaderService.load(newStart, newEnd));
    //
    // }
    //
    // count += loads.stream().map(load -> {
    // long value = load.join().longValue();
    // return value;
    // }).reduce(0L, Long::sum);
    //
    // // log.info("count={}", count);
    // }
    //
    // long endTime = System.currentTimeMillis();
    //
    // long timeTakenInMilliseconds = (endTime - startTime);
    //
    // long timeTakenInSecs = timeTakenInMilliseconds / 1000;
    //
    // long timeTakenInMins = timeTakenInSecs / 60;
    //
    // log.info("done! dbRowCount={}, biggestPropertyMasterKey={}, elasticRowCount={}, timeTaken={} secs => {} mins",
    // dbRowCount, biggestPropertyMasterKey, count.intValue(), timeTakenInSecs,
    // timeTakenInMins);
    // }
    //
    // removeLogs(database);
    //
    // if (turnOffServerAfterRun) {
    // System.exit(1);
    // }
    //
    // try {
    // Thread.sleep(2000);
    // } catch (InterruptedException e) {
    // log.warn(e.getLocalizedMessage());
    // }
    //
    // // propertyES.getPropertyTypeCounts(database);
    // //
    // // propertyES.getPropertySubTypeCounts(database);
    //
    // }
    //
    // private void removeLogs(String database) {
    // Path path = Paths.get("logs/"+database);
    //
    // // read java doc, Files.walk need close the resources.
    // // try-with-resources to ensure that the stream's open directories are closed
    // try (Stream<Path> walk = Files.walk(path)) {
    // walk.sorted(Comparator.reverseOrder()).forEach(p -> {
    // try {
    // Files.delete(p);
    // } catch (IOException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    // });
    // } catch (IOException e1) {
    // // TODO Auto-generated catch block
    // e1.printStackTrace();
    // }
    // }
}
