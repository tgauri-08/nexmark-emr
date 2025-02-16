package com.github.nexmark.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class SQLFileGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(SQLFileGenerator.class);
    private static final List<String> ALL_QUERIES = Arrays.asList(
        "q0", "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10",
        "q11", "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20",
        "q21", "q22"
    );
    
    private final Path queryLocation;
    
    public SQLFileGenerator(String queryLocation) {
        this.queryLocation = Paths.get(queryLocation);
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
    
    private List<String> initializeSqlFileLines(Map<String, String> vars, File sqlFile) throws IOException {
        List<String> lines = Files.readAllLines(sqlFile.toPath());
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
        allLines.addAll(initializeSqlFileLines(vars, new File(queryLocation.toFile(), "ddl_gen.sql")));
        allLines.addAll(initializeSqlFileLines(vars, new File(queryLocation.toFile(), "ddl_kafka.sql")));
        allLines.addAll(initializeSqlFileLines(vars, new File(queryLocation.toFile(), "ddl_views.sql")));
        allLines.addAll(initializeSqlFileLines(vars, new File(queryLocation.toFile(), queryName + ".sql")));
        
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
                LOG.error("Error generating SQL file for query {}: {}", queryName, e.getMessage());
            }
        }
        
        LOG.info("Completed generating all SQL files");
    }
    
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java SQLFileGenerator <query-location>");
            System.out.println("Example: java SQLFileGenerator /path/to/queries");
            System.exit(1);
        }
        
        String queryLocation = args[0];
        SQLFileGenerator generator = new SQLFileGenerator(queryLocation);
        generator.generateAllQueryFiles();
    }
}
