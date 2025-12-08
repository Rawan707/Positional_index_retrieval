# Build Instructions

## Prerequisites
- Java 17 or higher
- Apache Maven 3.6+
- Apache Spark 3.5.0 (for standalone execution)

## Build Commands

### Clean and Compile
```bash
mvn clean compile
```

### Run Tests
```bash
mvn test
```

### Package JAR
```bash
mvn clean package
```

### Run Application (Local Mode)
```bash
mvn exec:java -Dexec.mainClass="com.ir.spark.SearchEngineSpark"
```

### Create Fat JAR with Dependencies
```bash
mvn clean package shade:shade
```

### Run with Spark Submit
```bash
spark-submit --class com.ir.spark.SearchEngineSpark \
  --master local[*] \
  target/spark-information-retrieval-system-1.0.0.jar
```

### Run in Cluster Mode
```bash
spark-submit --class com.ir.spark.SearchEngineSpark \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 2g \
  --executor-cores 2 \
  --num-executors 3 \
  target/spark-information-retrieval-system-1.0.0.jar
```

## Development Commands

### Install Dependencies
```bash
mvn dependency:resolve
```

### Generate Sources JAR
```bash
mvn source:jar
```

### Generate Javadoc
```bash
mvn javadoc:javadoc
```

### Run with Debugging
```bash
mvn exec:java -Dexec.mainClass="com.ir.spark.SearchEngineSpark" -Dexec.args="-Xdebug"
```