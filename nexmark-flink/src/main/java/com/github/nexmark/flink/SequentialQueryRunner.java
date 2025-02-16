package com.github.nexmark.flink;

import com.github.nexmark.flink.metric.FlinkRestClient;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class SequentialQueryRunner {
    private static final Logger LOG = LoggerFactory.getLogger(SequentialQueryRunner.class);
    private final Path flinkDist;
    private final Path sqlFilesDir;
    private final String jmAddress;
    private final int jmPort;
    private final String masterFilePath;
    private final FlinkRestClient flinkRestClient;

    public SequentialQueryRunner(String flinkHome, String sqlFilesDir, String jmAddress, int jmPort, String masterFilePath) {
        this.flinkDist = Paths.get(flinkHome);
        this.sqlFilesDir = Paths.get(sqlFilesDir);
        this.jmAddress = jmAddress;
        this.jmPort = jmPort;
        this.masterFilePath = masterFilePath;
        this.flinkRestClient = new FlinkRestClient(jmAddress, jmPort);
    }

    private static Tuple2<String, Integer> startYarnSessionAndGetAddress() throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(
            "flink-yarn-session",
            "-d"             // Detached mode
        );

        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();

        // Read the output to get the Web Interface URL
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        String webInterfaceUrl = null;
        while ((line = reader.readLine()) != null) {
            LOG.info("YARN Session output: {}", line);
            if (line.contains("Web Interface:")) {
                webInterfaceUrl = line.split("Web Interface:")[1].trim();
                break;
            }
        }

        process.waitFor();

        if (webInterfaceUrl == null) {
            throw new RuntimeException("Could not find Web Interface URL in YARN session output");
        }

        // Parse the URL to get host and port
        String[] parts = webInterfaceUrl.split("://")[1].split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        return new Tuple2<>(host, port);
    }

    public void runAllQueries() {
        for (int i = 0; i <= 22; i++) {
            String queryName = "q" + i;
            // Skip q6 and q9
            if (i == 6 || i == 9) {
                LOG.info("Skipping query {}", queryName);
                continue;
            }
            runSingleQuery(queryName);
        }
    }

    public void runSpecificQuery(String queryName) {
   	  runSingleQuery(queryName);
    }

    private void runSingleQuery(String queryName) {
        try {
            Path sqlFile = sqlFilesDir.resolve("nexmark-" + queryName + ".sql");
            if (!Files.exists(sqlFile)) {
                LOG.error("SQL file not found for query: {}", queryName);
                logToMasterFile(queryName, "N/A", "FAILED", "SQL file not found");
                return;
            }

            LOG.info("Starting query: {}", queryName);
            submitSqlJob(sqlFile);
            
            // Wait for job to start and get job ID
            Thread.sleep(10000);  
            String jobId = null;
            int attempts = 0;
            while (attempts < 60) {  
                jobId = flinkRestClient.getCurrentJobId();
                if (jobId != null && !jobId.isEmpty()) {
                    break;
                }
                Thread.sleep(1000);
                attempts++;
            }

            if (jobId == null || jobId.isEmpty()) {
                LOG.error("Failed to get job ID for query: {}", queryName);
                logToMasterFile(queryName, "N/A", "FAILED", "Could not get job ID");
                return;
            }

            LOG.info("Started job {} for query {}", jobId, queryName);
            
            // Wait for job to actually start running
            attempts = 0;
            while (attempts < 60) {  // Increased to 60 seconds
                if (flinkRestClient.isJobRunning()) {
                    break;
                }
                Thread.sleep(1000);
                attempts++;
            }

            if (!flinkRestClient.isJobRunning()) {
                LOG.error("Job {} for query {} failed to start running", jobId, queryName);
                logToMasterFile(queryName, jobId, "FAILED", "Job did not start running");
                return;
            }

            // Monitor job until completion
            while (!flinkRestClient.isJobCancellingOrFinished()) {
                Thread.sleep(5000);  // Check every 5 seconds
                LOG.info("Job {} for query {} is still running", jobId, queryName);
            }

            // Double check final status
            Thread.sleep(5000);
            String finalStatus = flinkRestClient.isJobCancellingOrFinished() ? "SUCCESS" : "FAILED";
            
            String curlOutput = executeCurlCommand(jobId);
            logToMasterFile(queryName, jobId, finalStatus, curlOutput);

            LOG.info("Completed query: {} with status: {}", queryName, finalStatus);

        } catch (Exception e) {
            LOG.error("Error running query: " + queryName, e);
            logToMasterFile(queryName, "N/A", "FAILED", e.getMessage());
        }
    }

    private void submitSqlJob(Path sqlFile) throws IOException, InterruptedException {
        Path flinkBin = flinkDist.resolve("bin");
        List<String> command = new ArrayList<>();
        command.add(flinkBin.resolve("sql-client.sh").toAbsolutePath().toString());
        command.add("embedded");
        command.add("-f");
        command.add(sqlFile.toAbsolutePath().toString());

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.inheritIO();
        Process process = processBuilder.start();
        process.waitFor();
    }

    private String executeCurlCommand(String jobId) throws IOException, InterruptedException {
        String curlCommand = String.format("curl %s:%d/jobs/%s", jmAddress, jmPort, jobId);
        Process process = Runtime.getRuntime().exec(curlCommand);
        process.waitFor();
        return new String(process.getInputStream().readAllBytes());
    }

    private void logToMasterFile(String queryName, String jobId, String status, String output) {
        LocalDateTime now = LocalDateTime.now();
        String timestamp = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String logEntry = String.format("[%s] QUERY: %s JOB_ID: %s STATUS: %s OUTPUT: %s\n",
                timestamp, queryName, jobId, status, output);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(masterFilePath, true))) {
            writer.write(logEntry);
        } catch (IOException e) {
            LOG.error("Error writing to master file", e);
        }
    }

    public static void main(String[] args) {
      if (args.length < 2 || args.length > 3) {
          System.out.println("Usage: java SequentialQueryRunner <flink-home> <sql-files-dir> [query-name]");
          System.out.println("If query-name is not provided, all queries will be run");
          System.out.println("Example: java SequentialQueryRunner /usr/lib/flink /tmp q5");
          System.exit(1);
      }
  
      String flinkHome = args[0];
      String sqlFilesDir = args[1];
      String specificQuery = args.length == 3 ? args[2] : null;
      
      LocalDateTime now = LocalDateTime.now();
      String dateTime = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HHmmss"));
      String masterFilePath = "/tmp/results-" + dateTime + ".txt";
  
      try {
          Tuple2<String, Integer> address = startYarnSessionAndGetAddress();
          String jmAddress = address.f0;
          int jmPort = address.f1;
  
          LOG.info("Started Flink session at {}:{}", jmAddress, jmPort);
          LOG.info("Results will be written to: {}", masterFilePath);
  
          SequentialQueryRunner runner = new SequentialQueryRunner(
              flinkHome, sqlFilesDir, jmAddress, jmPort, masterFilePath);
  
          if (specificQuery != null) {
              LOG.info("Running specific query: {}", specificQuery);
              runner.runSpecificQuery(specificQuery);
          } else {
              LOG.info("Running all queries");
              runner.runAllQueries();
          }
  
      } catch (Exception e) {
          LOG.error("Failed to start YARN session or run queries", e);
          System.exit(1);
      }
  }

}
