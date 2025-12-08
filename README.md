# Information Retrieval System with Apache Spark

A complete Java Spark application that implements a positional index-based information retrieval system with TF-IDF scoring and phrase query support.

## Features

### Part 1: Positional Index Builder
- Reads documents from `dataset/` folder (1.txt to 10.txt)
- Uses Apache Spark RDDs for distributed processing
- Builds positional index with exact format: `<term    doc1: pos1, pos2 ; doc2: pos1, pos2 ; ... >`
- Saves output to `output/` folder using `saveAsTextFile()`

### Part 2: TF-IDF Computation
- **Term Frequency**: Uses formula `tf_weight = 1 + log10(tf)`
- **IDF Calculation**: Uses formula `idf = log10(N / df)`
- **TF-IDF Matrix**: Complete matrix for all terms and documents
- Uses output from Part 1 for processing

### Part 3: Phrase Query Search Engine
- Supports multiple query types:
  - Simple phrases: `"information retrieval"`
  - AND queries: `"machine learning" AND "artificial intelligence"`
  - OR queries: `"computer science" OR "software engineering"`
  - NOT queries: `"data mining" AND NOT "statistics"`
- Cosine similarity computation
- Document ranking by relevance score

## Project Structure

```
IRSystem/
├── pom.xml                                    # Maven configuration
├── README.md                                  # This file
├── BUILD.md                                   # Build instructions
├── .gitignore                                 # Git ignore rules
├── src/
│   ├── main/
│   │   ├── java/com/ir/spark/
│   │   │   └── SearchEngineSpark.java         # Main Spark application
│   │   └── resources/
│   │       ├── dataset/                       # Input documents
│   │       │   ├── 1.txt                     # Information Retrieval
│   │       │   ├── 2.txt                     # Machine Learning
│   │       │   ├── 3.txt                     # Data Mining
│   │       │   ├── 4.txt                     # Artificial Intelligence
│   │       │   ├── 5.txt                     # Computer Science
│   │       │   ├── 6.txt                     # Big Data
│   │       │   ├── 7.txt                     # Database Systems
│   │       │   ├── 8.txt                     # Natural Language Processing
│   │       │   ├── 9.txt                     # Software Engineering
│   │       │   └── 10.txt                    # Web Development
│   │       └── log4j2.xml                     # Logging configuration
│   └── test/
│       └── java/com/ir/spark/
│           └── SearchEngineSparkTest.java     # Unit tests
├── target/                                    # Maven build output
├── logs/                                      # Application logs
└── output/                                    # Spark output (created at runtime)
```

## Requirements

- Java 17 or higher
- Apache Spark 3.x
- Spark Java API libraries

## Compilation and Execution

### Using Maven:
```bash
# Clean and compile
mvn clean compile

# Run tests
mvn test

# Package JAR with dependencies
mvn clean package

# Run locally
mvn exec:java -Dexec.mainClass="com.ir.spark.SearchEngineSpark"
```

### Using spark-submit:
```bash
# After Maven packaging
spark-submit --class com.ir.spark.SearchEngineSpark \
  --master local[*] \
  target/spark-information-retrieval-system-1.0.0.jar
```

### Using IDE:
1. Import as Maven project
2. Run the `main` method in `SearchEngineSpark.java`
3. All dependencies will be automatically resolved

## Output Format

### Positional Index
```
<term    doc1: 1, 5, 12 ; doc2: 3, 8 ; doc3: 2, 15, 20 >
```

### TF Table
```
Term            Doc1        Doc2        Doc3        ...
information     1.3010      0.0000      1.4771      ...
retrieval       1.0000      0.0000      1.0000      ...
```

### Query Results
```
Query: "information retrieval"
Query TF Analysis:
Term            Raw TF      TF (1+log)     IDF         TF*IDF
information     1           1.0000         0.3010      0.3010
retrieval       1           1.0000         0.4771      0.4771
Query length (norm): 0.5732

Similarity Computation:
Document 1 similarity: 0.854321
Document 3 similarity: 0.673451

Ranked Results:
Doc ID  Score
1       0.854321
3       0.673451
```

## Technical Implementation

### Core Classes:
- `SearchEngineSpark`: Main application class
- `DocumentPosition`: Stores document ID and term positions
- `TermPosition`: Intermediate class for RDD processing
- `DocumentScore`: Helper class for ranking results

### Key Methods:
- `buildPositionalIndex()`: Creates positional index from documents
- `loadPositionalIndexFromOutput()`: Loads saved index from Part 1
- `computeTF()`: Calculates term frequency weights
- `computeIDF()`: Calculates inverse document frequency
- `computeTFIDF()`: Builds TF-IDF matrix
- `processPhraseQuery()`: Handles phrase queries and ranking
- `cosineSimilarity()`: Computes document similarity scores

## Dataset

The application includes 10 sample documents covering computer science topics:
1. Information Retrieval
2. Machine Learning
3. Data Mining
4. Artificial Intelligence
5. Computer Science
6. Big Data
7. Database Systems
8. Natural Language Processing
9. Software Engineering
10. Web Development

## Features Implemented

✅ Pure Spark RDD implementation (no DataFrames)  
✅ Positional indexing with exact output format  
✅ TF-IDF computation with correct formulas  
✅ Phrase query support with Boolean operators  
✅ Cosine similarity and document ranking  
✅ Part 2 uses output from Part 1  
✅ Complete error handling and logging  
✅ Ready for spark-submit execution  

## Author

Created for Information Retrieval System assignment using Apache Spark Java API.