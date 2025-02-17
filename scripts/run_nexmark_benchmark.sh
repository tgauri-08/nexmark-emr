#!/bin/bash

echo "=== Nexmark Benchmark Automation Script ==="

# Set base directories
FLINK_HOME="/usr/lib/flink"
NEXMARK_HOME="/home/hadoop/nexmark-emr"
SCRIPTS_DIR="/home/hadoop/nexmark-emr/scripts"
QUERIES_DIR="$NEXMARK_HOME/nexmark-flink/src/main/resources/queries"

# Color-coding output for more visibility
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}1. Building Java Project${NC}"
cd $NEXMARK_HOME
mvn clean install
if [ $? -ne 0 ]; then
    echo -e "${RED}Maven build failed${NC}"
    exit 1
fi

# Check if SQL files exist in /tmp
echo -e "\n${GREEN}2. Checking for SQL Files${NC}"
if ls /tmp/nexmark-q*.sql 1> /dev/null 2>&1; then
    echo "SQL files already exist in /tmp, skipping generation"
else
    echo "No SQL files found in /tmp, generating now..."
    java -cp "$NEXMARK_HOME/nexmark-flink/target/nexmark-flink-0.3-SNAPSHOT.jar:$FLINK_HOME/lib/*" \
    com.github.nexmark.flink.SQLFileGenerator \
    "$QUERIES_DIR"
    if [ $? -ne 0 ]; then
        echo -e "${RED}SQL file generation failed${NC}"
        exit 1
    fi
fi

echo -e "\n${GREEN}3. Running Queries${NC}"
java -cp "target/nexmark-flink-0.3-SNAPSHOT.jar:/usr/lib/flink/lib/*" \
    com.github.nexmark.flink.SequentialQueryRunner \
    /usr/lib/flink \
    /tmp
if [ $? -ne 0 ]; then
    echo -e "${RED}Query execution failed${NC}"
    exit 1
fi

echo -e "\n${GREEN}4. Processing Results with Python Script${NC}"
# Activate virtual environment
cd $SCRIPTS_DIR
source venv/bin/activate

# Get latest results file
RESULTS_FILE=$(ls -t /tmp/results-* | head -n1)
echo "Processing results file: $RESULTS_FILE"

# Run Python script
python process_results.py "$RESULTS_FILE"
if [ $? -ne 0 ]; then
    echo -e "${RED}Python processing failed${NC}"
    deactivate
    exit 1
fi

# Deactivate virtual environment
deactivate

echo -e "\n${GREEN}5. Getting Latest Excel File${NC}"
EXCEL_FILE=$(ls -t /tmp/query_metrics_* | head -n1)
echo "Excel file generated: $EXCEL_FILE"

# Optional: Upload to S3
# aws s3 cp "$EXCEL_FILE" s3://your-bucket/results/

echo -e "\n${GREEN}=== Benchmark Complete ===${NC}"
echo "Results file: $RESULTS_FILE"
echo "Excel file: $EXCEL_FILE"
