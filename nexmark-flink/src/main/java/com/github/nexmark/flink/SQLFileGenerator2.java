package com.github.nexmark.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class SQLFileGenerator2 {
    private static final Logger LOG = LoggerFactory.getLogger(SQLFileGenerator.class);
    private static final List<String> ALL_QUERIES = Arrays.asList(
        "q0", "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10",
        "q11", "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20",
        "q21", "q22"
    );
    
    private final String queryLocation;
    
    public SQLFileGenerator2(String queryLocation) {
        this.queryLocation = queryLocation;
    }
    
    private Map<String, String> initializeVariables() {
        Map<String, String> varsMap = new HashMap<>();
        varsMap.put("TPS", "1000000");
        varsMap.put("EVENTS_NUM", "100000000");
        varsMap.put("PERSON_PROPORTION", "1");
        varsMap.put("AUCTION_PROPORTION", "1");
        varsMap.put("BID_PROPORTION", "1");
        varsMap.put("NEXMARK_TABLE", "datagen");
        varsMap.put("BOOTSTRAP_SERVERS", "");
        return varsMap;
    }
    
    private List<String> readLinesFromResource(String resourcePath) throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            
            List<String> lines = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            return lines;
        }
    }
    
    private List<String> initializeSqlFileLines(Map<String, String> vars, String resourceName) throws IOException {
        String resourcePath = queryLocation + "/" + resourceName;
        List<String> lines = readLinesFromResource(resourcePath);
        List<String> result = new ArrayList<>();
        for (String line : lines) {
            for (Map.Entry<String, String> var : vars.entrySet()) {
                line = line.replace("${" + var.getKey() + "}", var.getValue());
            }
            result.add(line);
        }
        return result;
    }
    
    private List<String> generateQueryContent(String queryName) throws IOException {
        Map<String, String> vars = initializeVariables();
        List<String> allLines = new ArrayList<>();
        
        // Add job name
        allLines.add("SET pipeline.name = 'Nexmark " + queryName.toUpperCase() + "';");
        
        // Add required SQL content
        allLines.addAll(initializeSqlFileLines(vars, "ddl_gen.sql"));
        allLines.addAll(initializeSqlFileLines(vars, "ddl_kafka.sql"));
        allLines.addAll(initializeSqlFileLines(vars, "ddl_views.sql"));
        allLines.addAll(initializeSqlFileLines(vars, queryName + ".sql"));
        
        return allLines;
    }
    
    public void generateAllQueryFiles() {
        LOG.info("Starting to generate SQL files for all queries");
        
        for (String queryName : ALL_QUERIES) {
            try {
                List<String> queryContent = generateQueryContent(queryName);
                Path outputFile = Paths.get("/tmp/nexmark-" + queryName + ".sql");
                Files.write(outputFile, queryContent);
                LOG.info("Generated SQL file for {}: {}", queryName, outputFile);
            } catch (IOException e) {
                LOG.error("Error generating SQL file for query {}: {}", queryName, queryLocation + "/" + queryName + ".sql", e);
            }
        }
        
        LOG.info("Completed generating all SQL files");
    }
    
    public static void main(String[] args) {
        // No need for command line arguments anymore
        SQLFileGenerator2 generator = new SQLFileGenerator2("queries");
        generator.generateAllQueryFiles();
    }
}
