package com.ir.spark;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SearchEngineSpark implements Serializable {
    
    private JavaSparkContext sparkContext;
    private Map<String, List<DocumentPosition>> positionalIndex;
    private Map<String, Map<Integer, Double>> tfMatrix;
    private Map<String, Double> idfMap;
    private Map<String, Map<Integer, Double>> tfidfMatrix;
    private int totalDocuments = 10;
    
    // Document Position class to store document ID and positions
    public static class DocumentPosition implements Serializable {
        public int docId;
        public List<Integer> positions;
        
        public DocumentPosition(int docId, List<Integer> positions) {
            this.docId = docId;
            this.positions = positions;
        }
        
        @Override
        public String toString() {
            return "doc" + docId + ": " + positions.stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining(", "));
        }
    }
    
    // Term Position class for intermediate processing
    public static class TermPosition implements Serializable {
        public String term;
        public int docId;
        public int position;
        
        public TermPosition(String term, int docId, int position) {
            this.term = term;
            this.docId = docId;
            this.position = position;
        }
    }

    // Static PairFunction to read files into (docId, content) pairs.
    // Declared static and Serializable to avoid capturing the outer SearchEngineSpark instance
    private static class FileToPair implements PairFunction<String, Integer, String>, Serializable {
        @Override
        public Tuple2<Integer, String> call(String filePath) throws Exception {
            try {
                if (!Files.exists(Paths.get(filePath))) {
                    System.err.println("File does not exist: " + filePath);
                    return new Tuple2<>(0, "");
                }
                String content = new String(Files.readAllBytes(Paths.get(filePath)));
                // Extract document number from filename
                String filename = Paths.get(filePath).getFileName().toString();
                int docId = Integer.parseInt(filename.replace(".txt", ""));
                System.out.println("Successfully read: " + filename + " (Doc " + docId + ")");
                return new Tuple2<>(docId, content.toLowerCase());
            } catch (Exception e) {
                System.err.println("Error reading file: " + filePath + " - " + e.getMessage());
                return new Tuple2<>(0, "");
            }
        }
    }

    // Static FlatMapFunction to convert (docId, content) into TermPosition entries
    private static class TermPositionFlatMap implements FlatMapFunction<Tuple2<Integer, String>, TermPosition>, Serializable {
        @Override
        public Iterator<TermPosition> call(Tuple2<Integer, String> docContent) throws Exception {
            List<TermPosition> termPositions = new ArrayList<>();
            String[] tokens = docContent._2().split("[\\s\\p{Punct}]+");

            for (int pos = 0; pos < tokens.length; pos++) {
                String term = tokens[pos].trim();
                if (!term.isEmpty() && term.length() > 1) {
                    termPositions.add(new TermPosition(term, docContent._1(), pos + 1));
                }
            }
            return termPositions.iterator();
        }
    }

    // Static PairFunction to convert TermPosition to (term, TermPosition) pair
    private static class TermToPair implements PairFunction<TermPosition, String, TermPosition>, Serializable {
        @Override
        public Tuple2<String, TermPosition> call(TermPosition tp) throws Exception {
            return new Tuple2<>(tp.term, tp);
        }
    }

    // Static Function to convert grouped TermPosition iterable into List<DocumentPosition>
    private static class GroupToDocPositionsFunction implements Function<Iterable<TermPosition>, List<DocumentPosition>>, Serializable {
        @Override
        public List<DocumentPosition> call(Iterable<TermPosition> termPositions) throws Exception {
            Map<Integer, List<Integer>> docPositions = new HashMap<>();

            for (TermPosition tp : termPositions) {
                docPositions.computeIfAbsent(tp.docId, k -> new ArrayList<>()).add(tp.position);
            }

            List<DocumentPosition> result = new ArrayList<>();
            for (Map.Entry<Integer, List<Integer>> entry : docPositions.entrySet()) {
                Collections.sort(entry.getValue());
                result.add(new DocumentPosition(entry.getKey(), entry.getValue()));
            }

            // Sort by document ID
            result.sort(Comparator.comparingInt(dp -> dp.docId));
            return result;
        }
    }
    
    public SearchEngineSpark() {
        SparkConf conf = new SparkConf()
                .setAppName("SearchEngineSpark")
                .setMaster("local[*]");
        
        sparkContext = new JavaSparkContext(conf);
        positionalIndex = new HashMap<>();
        tfMatrix = new HashMap<>();
        idfMap = new HashMap<>();
        tfidfMatrix = new HashMap<>();
    }
    
    public void buildPositionalIndex() {
        System.out.println("=== PART 1: Building Positional Index ===");
        
        // Read all documents
        List<String> inputFiles = new ArrayList<>();
        String resourcePath = System.getProperty("user.dir") + "/src/main/resources/dataset/";
        for (int i = 1; i <= 10; i++) {
            inputFiles.add(resourcePath + i + ".txt");
        }
        
        JavaRDD<String> allFiles = sparkContext.parallelize(inputFiles);
        
        // Read file contents with document IDs
        JavaPairRDD<Integer, String> documentContents = allFiles.mapToPair(
            new FileToPair()
        );
        
        // Tokenize and create term positions
        JavaRDD<TermPosition> termPositions = documentContents.flatMap(
            new TermPositionFlatMap()
        );
        
        // Group by term and document
        JavaPairRDD<String, Iterable<TermPosition>> groupedByTerm = termPositions
            .mapToPair(new TermToPair())
            .groupByKey();
        
        // Build positional index
        JavaPairRDD<String, List<DocumentPosition>> positionalIndexRDD = groupedByTerm.mapValues(
            new GroupToDocPositionsFunction()
        );
        
        // Collect and store positional index
        Map<String, List<DocumentPosition>> collectedIndex = positionalIndexRDD.collectAsMap();
        this.positionalIndex = new HashMap<>(collectedIndex);
        
        // Print positional index
        System.out.println("\nPositional Index:");
        List<String> sortedTerms = new ArrayList<>(positionalIndex.keySet());
        Collections.sort(sortedTerms);
        
        for (String term : sortedTerms) {
            List<DocumentPosition> docPositions = positionalIndex.get(term);
            String positionsStr = docPositions.stream()
                    .map(DocumentPosition::toString)
                    .collect(Collectors.joining(" ; "));
            System.out.println("<" + term + "    " + positionsStr + " >");
        }
        
        // Save to a single output file on the driver (avoid Hadoop native issues on Windows)
        List<String> outLines = sortedTerms.stream().map(term -> {
            List<DocumentPosition> docPositions = positionalIndex.get(term);
            String positionsStr = docPositions.stream()
                    .map(DocumentPosition::toString)
                    .collect(Collectors.joining(" ; "));
            return "<" + term + "    " + positionsStr + " >";
        }).collect(Collectors.toList());

        String outputDir = System.getProperty("user.dir") + "/output";
        try {
            java.nio.file.Path outPath = Paths.get(outputDir);
            java.nio.file.Files.createDirectories(outPath);
            java.nio.file.Path outFile = outPath.resolve("positional_index.txt");
            java.nio.file.Files.write(outFile, outLines);
            System.out.println("\nPositional index saved to: " + outFile.toString());
        } catch (Exception e) {
            System.err.println("Error saving positional index to file: " + e.getMessage());
        }
    }
    
    public void loadPositionalIndexFromOutput() {
        System.out.println("\n=== Loading Positional Index from Output File ===");
        
        try {
            // Read the saved positional index output from the driver-side file
            String outputDir = System.getProperty("user.dir") + "/output";
            java.nio.file.Path filePath = Paths.get(outputDir).resolve("positional_index.txt");
            List<String> indexLines = java.nio.file.Files.readAllLines(filePath);
            
            for (String line : indexLines) {
                if (line.startsWith("<") && line.endsWith(" >")) {
                    // Parse: <term    doc1: pos1, pos2 ; doc2: pos1, pos2 ; ... >
                    String content = line.substring(1, line.length() - 2); // Remove < and >
                    String[] parts = content.split("\\s{4}", 2); // Split on 4 spaces
                    
                    if (parts.length == 2) {
                        String term = parts[0].trim();
                        String docParts = parts[1];
                        
                        List<DocumentPosition> docPositions = new ArrayList<>();
                        String[] docEntries = docParts.split(" ; ");
                        
                        for (String docEntry : docEntries) {
                            String[] docData = docEntry.split(": ");
                            if (docData.length == 2) {
                                int docId = Integer.parseInt(docData[0].replace("doc", ""));
                                String[] positions = docData[1].split(", ");
                                List<Integer> posList = new ArrayList<>();
                                for (String pos : positions) {
                                    posList.add(Integer.parseInt(pos.trim()));
                                }
                                docPositions.add(new DocumentPosition(docId, posList));
                            }
                        }
                        positionalIndex.put(term, docPositions);
                    }
                }
            }
            System.out.println("Successfully loaded " + positionalIndex.size() + " terms from output file.");
        } catch (Exception e) {
            System.err.println("Error loading positional index from output: " + e.getMessage());
            // Fallback to building from scratch if loading fails
            System.out.println("Using existing positional index from Part 1.");
        }
    }
    
    public void computeTF() {
        System.out.println("\n=== PART 2.1: Computing Term Frequency ===");
        
        // Ensure we have positional index (either from Part 1 or loaded from output)
        if (positionalIndex.isEmpty()) {
            System.out.println("Loading positional index from output file...");
            loadPositionalIndexFromOutput();
        }
        
        // Calculate raw term frequencies
        Map<String, Map<Integer, Integer>> rawTF = new HashMap<>();
        
        for (Map.Entry<String, List<DocumentPosition>> entry : positionalIndex.entrySet()) {
            String term = entry.getKey();
            Map<Integer, Integer> docFreq = new HashMap<>();
            
            for (DocumentPosition dp : entry.getValue()) {
                docFreq.put(dp.docId, dp.positions.size());
            }
            rawTF.put(term, docFreq);
        }
        
        // Apply TF weight formula: 1 + log10(tf)
        for (Map.Entry<String, Map<Integer, Integer>> termEntry : rawTF.entrySet()) {
            String term = termEntry.getKey();
            Map<Integer, Double> tfWeights = new HashMap<>();
            
            for (Map.Entry<Integer, Integer> docEntry : termEntry.getValue().entrySet()) {
                int docId = docEntry.getKey();
                int rawFreq = docEntry.getValue();
                double tfWeight = 1.0 + Math.log10(rawFreq);
                tfWeights.put(docId, tfWeight);
            }
            tfMatrix.put(term, tfWeights);
        }
        
        // Print TF table
        System.out.println("\nTerm Frequency Table (TF = 1 + log10(tf)):");
        System.out.printf("%-15s", "Term");
        for (int i = 1; i <= totalDocuments; i++) {
            System.out.printf("%-12s", "Doc" + i);
        }
        System.out.println();
        System.out.println("=" + "=".repeat(15 + 12 * totalDocuments));
        
        List<String> sortedTerms = new ArrayList<>(tfMatrix.keySet());
        Collections.sort(sortedTerms);
        
        for (String term : sortedTerms) {
            System.out.printf("%-15s", term);
            Map<Integer, Double> docTFs = tfMatrix.get(term);
            for (int i = 1; i <= totalDocuments; i++) {
                Double tf = docTFs.get(i);
                if (tf != null) {
                    System.out.printf("%-12.4f", tf);
                } else {
                    System.out.printf("%-12s", "0.0000");
                }
            }
            System.out.println();
        }
    }
    
    public void computeIDF() {
        System.out.println("\n=== PART 2.2: Computing IDF ===");
        
        for (String term : positionalIndex.keySet()) {
            int df = positionalIndex.get(term).size(); // document frequency
            double idf = Math.log10((double) totalDocuments / df);
            idfMap.put(term, idf);
        }
        
        // Print IDF values
        System.out.println("\nIDF Values (IDF = log10(N/df)):");
        System.out.printf("%-15s%-12s%-12s%-12s%n", "Term", "DF", "N/DF", "IDF");
        System.out.println("=" + "=".repeat(51));
        
        List<String> sortedTerms = new ArrayList<>(idfMap.keySet());
        Collections.sort(sortedTerms);
        
        for (String term : sortedTerms) {
            int df = positionalIndex.get(term).size();
            double ratio = (double) totalDocuments / df;
            double idf = idfMap.get(term);
            System.out.printf("%-15s%-12d%-12.4f%-12.4f%n", term, df, ratio, idf);
        }
    }
    
    public void computeTFIDF() {
        System.out.println("\n=== PART 2.3: Computing TF-IDF Matrix ===");
        
        // Calculate TF-IDF for each term and document
        for (String term : tfMatrix.keySet()) {
            Map<Integer, Double> docTFIDFs = new HashMap<>();
            Map<Integer, Double> docTFs = tfMatrix.get(term);
            double idf = idfMap.get(term);
            
            for (Map.Entry<Integer, Double> entry : docTFs.entrySet()) {
                int docId = entry.getKey();
                double tf = entry.getValue();
                double tfidf = tf * idf;
                docTFIDFs.put(docId, tfidf);
            }
            tfidfMatrix.put(term, docTFIDFs);
        }
        
        // Print TF-IDF matrix
        System.out.println("\nTF-IDF Matrix:");
        System.out.printf("%-15s", "Term");
        for (int i = 1; i <= totalDocuments; i++) {
            System.out.printf("%-12s", "d" + i);
        }
        System.out.println();
        System.out.println("=" + "=".repeat(15 + 12 * totalDocuments));
        
        List<String> sortedTerms = new ArrayList<>(tfidfMatrix.keySet());
        Collections.sort(sortedTerms);
        
        for (String term : sortedTerms) {
            System.out.printf("%-15s", term);
            Map<Integer, Double> docTFIDFs = tfidfMatrix.get(term);
            for (int i = 1; i <= totalDocuments; i++) {
                Double tfidf = docTFIDFs.get(i);
                if (tfidf != null) {
                    System.out.printf("%-12.4f", tfidf);
                } else {
                    System.out.printf("%-12s", "0.0000");
                }
            }
            System.out.println();
        }
    }
    
    public void processPhraseQuery(String query) {
        System.out.println("\n=== PART 2.4: Phrase Query Search Engine ===");
        System.out.println("Query: " + query);
        
        // Parse query and find matching documents
        Set<Integer> matchingDocs = findMatchingDocuments(query);
        
        if (matchingDocs.isEmpty()) {
            System.out.println("No documents found matching the query.");
            return;
        }
        
        System.out.println("Matching documents: " + matchingDocs);
        
        // Extract query terms
        List<String> queryTerms = extractQueryTerms(query);
        
        // Calculate query TF
        Map<String, Integer> queryTermFreq = new HashMap<>();
        for (String term : queryTerms) {
            queryTermFreq.put(term, queryTermFreq.getOrDefault(term, 0) + 1);
        }
        
        // Print query TF information
        System.out.println("\nQuery TF Analysis:");
        System.out.printf("%-15s%-12s%-15s%-12s%-12s%n", "Term", "Raw TF", "TF (1+log)", "IDF", "TF*IDF");
        System.out.println("=" + "=".repeat(66));
        
        Map<String, Double> queryTFIDF = new HashMap<>();
        double queryNorm = 0.0;
        
        for (String term : queryTermFreq.keySet()) {
            int rawTF = queryTermFreq.get(term);
            double tfWeight = 1.0 + Math.log10(rawTF);
            Double idf = idfMap.get(term);
            if (idf == null) idf = 0.0; // Term not in corpus
            
            double tfidf = tfWeight * idf;
            queryTFIDF.put(term, tfidf);
            queryNorm += tfidf * tfidf;
            
            System.out.printf("%-15s%-12d%-15.4f%-12.4f%-12.4f%n", term, rawTF, tfWeight, idf, tfidf);
        }
        
        queryNorm = Math.sqrt(queryNorm);
        System.out.printf("Query length (norm): %.4f%n", queryNorm);
        
        // Calculate cosine similarity for each matching document
        List<DocumentScore> documentScores = new ArrayList<>();
        
        System.out.println("\nSimilarity Computation:");
        for (int docId : matchingDocs) {
            double similarity = cosineSimilarity(queryTFIDF, docId, queryNorm);
            documentScores.add(new DocumentScore(docId, similarity));
            System.out.printf("Document %d similarity: %.6f%n", docId, similarity);
        }
        
        // Sort by similarity (descending)
        documentScores.sort((a, b) -> Double.compare(b.score, a.score));
        
        // Print ranked results
        System.out.println("\nRanked Results:");
        System.out.printf("%-8s%-12s%n", "Doc ID", "Score");
        System.out.println("=" + "=".repeat(20));
        for (DocumentScore ds : documentScores) {
            System.out.printf("%-8d%-12.6f%n", ds.docId, ds.score);
        }
    }
    
    private Set<Integer> findMatchingDocuments(String query) {
        Set<Integer> result = new HashSet<>();
        
        // Handle different query types
        if (query.contains(" AND NOT ")) {
            String[] parts = query.split(" AND NOT ");
            String positivePhrase = parts[0].trim();
            String negativePhrase = parts[1].trim();
            
            Set<Integer> positiveDocs = findPhraseDocuments(positivePhrase);
            Set<Integer> negativeDocs = findPhraseDocuments(negativePhrase);
            
            result.addAll(positiveDocs);
            result.removeAll(negativeDocs);
            
        } else if (query.contains(" AND ")) {
            String[] phrases = query.split(" AND ");
            result.addAll(findPhraseDocuments(phrases[0].trim()));
            
            for (int i = 1; i < phrases.length; i++) {
                Set<Integer> phraseDocs = findPhraseDocuments(phrases[i].trim());
                result.retainAll(phraseDocs);
            }
            
        } else if (query.contains(" OR ")) {
            String[] phrases = query.split(" OR ");
            for (String phrase : phrases) {
                result.addAll(findPhraseDocuments(phrase.trim()));
            }
            
        } else {
            // Simple phrase query
            result.addAll(findPhraseDocuments(query));
        }
        
        return result;
    }
    
    private Set<Integer> findPhraseDocuments(String phrase) {
        // Remove quotes if present
        phrase = phrase.replaceAll("\"", "").trim();
        String[] terms = phrase.toLowerCase().split("\\s+");
        
        if (terms.length == 1) {
            // Single term query
            String term = terms[0];
            Set<Integer> docs = new HashSet<>();
            if (positionalIndex.containsKey(term)) {
                for (DocumentPosition dp : positionalIndex.get(term)) {
                    docs.add(dp.docId);
                }
            }
            return docs;
        }
        
        // Multi-term phrase query
        Set<Integer> candidateDocs = new HashSet<>();
        
        // Get documents containing the first term
        if (positionalIndex.containsKey(terms[0])) {
            for (DocumentPosition dp : positionalIndex.get(terms[0])) {
                candidateDocs.add(dp.docId);
            }
        }
        
        // Check for phrase in each candidate document
        Set<Integer> matchingDocs = new HashSet<>();
        for (int docId : candidateDocs) {
            if (containsPhrase(docId, terms)) {
                matchingDocs.add(docId);
            }
        }
        
        return matchingDocs;
    }
    
    private boolean containsPhrase(int docId, String[] terms) {
        // Get positions for first term in this document
        List<Integer> firstTermPositions = new ArrayList<>();
        if (positionalIndex.containsKey(terms[0])) {
            for (DocumentPosition dp : positionalIndex.get(terms[0])) {
                if (dp.docId == docId) {
                    firstTermPositions.addAll(dp.positions);
                    break;
                }
            }
        }
        
        // For each position of the first term, check if subsequent terms follow
        for (int startPos : firstTermPositions) {
            boolean phraseFound = true;
            
            for (int i = 1; i < terms.length; i++) {
                int expectedPos = startPos + i;
                boolean termAtPosition = false;
                
                if (positionalIndex.containsKey(terms[i])) {
                    for (DocumentPosition dp : positionalIndex.get(terms[i])) {
                        if (dp.docId == docId && dp.positions.contains(expectedPos)) {
                            termAtPosition = true;
                            break;
                        }
                    }
                }
                
                if (!termAtPosition) {
                    phraseFound = false;
                    break;
                }
            }
            
            if (phraseFound) {
                return true;
            }
        }
        
        return false;
    }
    
    private List<String> extractQueryTerms(String query) {
        List<String> terms = new ArrayList<>();
        
        // Extract terms from different query types
        String processedQuery = query.replaceAll("\"", "")
                                    .replaceAll(" AND NOT ", " ")
                                    .replaceAll(" AND ", " ")
                                    .replaceAll(" OR ", " ");
        
        String[] tokens = processedQuery.toLowerCase().split("\\s+");
        for (String token : tokens) {
            token = token.trim();
            if (!token.isEmpty()) {
                terms.add(token);
            }
        }
        
        return terms;
    }
    
    private double cosineSimilarity(Map<String, Double> queryVector, int docId, double queryNorm) {
        double dotProduct = 0.0;
        double docNorm = 0.0;
        
        // Calculate document norm and dot product
        for (String term : tfidfMatrix.keySet()) {
            Map<Integer, Double> termTFIDFs = tfidfMatrix.get(term);
            Double docTFIDF = termTFIDFs.get(docId);
            if (docTFIDF != null) {
                docNorm += docTFIDF * docTFIDF;
                
                // Add to dot product if term is in query
                Double queryTFIDF = queryVector.get(term);
                if (queryTFIDF != null) {
                    dotProduct += queryTFIDF * docTFIDF;
                }
            }
        }
        
        docNorm = Math.sqrt(docNorm);
        
        if (queryNorm == 0.0 || docNorm == 0.0) {
            return 0.0;
        }
        
        return dotProduct / (queryNorm * docNorm);
    }
    
    private double normalizeVector(Map<String, Double> vector) {
        double norm = 0.0;
        for (double value : vector.values()) {
            norm += value * value;
        }
        return Math.sqrt(norm);
    }
    
    // Helper class for document scoring
    public static class DocumentScore {
        public int docId;
        public double score;
        
        public DocumentScore(int docId, double score) {
            this.docId = docId;
            this.score = score;
        }
    }
    
    public void shutdown() {
        if (sparkContext != null) {
            sparkContext.stop();
        }
    }
    
    public static void main(String[] args) {
        SearchEngineSpark searchEngine = new SearchEngineSpark();
        
        try {
            // Part 1: Build Positional Index from Dataset
            System.out.println("=== STARTING PART 1: Building Positional Index from Dataset ===");
            searchEngine.buildPositionalIndex();
            
            // Part 2: Use Output from Part 1 for TF-IDF Computation
            System.out.println("\n=== STARTING PART 2: Using Output from Part 1 for TF-IDF ===");
            // Load from output file to demonstrate using Part 1 output
            searchEngine.loadPositionalIndexFromOutput();
            
            searchEngine.computeTF();
            searchEngine.computeIDF();
            searchEngine.computeTFIDF();
            
            // Part 3: Phrase Query Examples
            String[] testQueries = {
                "information retrieval",
                "\"data mining\"",
                "\"machine learning\" AND \"artificial intelligence\"",
                "\"computer science\" OR \"information technology\"",
                "\"data science\" AND NOT \"statistics\""
            };
            
            for (String query : testQueries) {
                searchEngine.processPhraseQuery(query);
                System.out.println("\n" + "=".repeat(80));
            }
            
        } catch (Exception e) {
            System.err.println("Error in SearchEngineSpark: " + e.getMessage());
            e.printStackTrace();
        } finally {
            searchEngine.shutdown();
        }
    }
}