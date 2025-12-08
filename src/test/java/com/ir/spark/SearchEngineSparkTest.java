package com.ir.spark;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * Unit tests for SearchEngineSpark
 */
public class SearchEngineSparkTest {
    
    private SearchEngineSpark searchEngine;
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    
    @BeforeEach
    public void setUp() {
        searchEngine = new SearchEngineSpark();
        System.setOut(new PrintStream(outContent));
    }
    
    @AfterEach
    public void tearDown() {
        if (searchEngine != null) {
            searchEngine.shutdown();
        }
        System.setOut(originalOut);
    }
    
    @Test
    public void testSearchEngineInitialization() {
        assertNotNull(searchEngine, "SearchEngineSpark should be initialized");
    }
    
    @Test
    public void testPositionalIndexBuilding() {
        // Test that positional index can be built without errors
        assertDoesNotThrow(() -> {
            searchEngine.buildPositionalIndex();
        }, "Building positional index should not throw exceptions");
    }
    
    @Test
    public void testTFComputation() {
        // First build the positional index
        searchEngine.buildPositionalIndex();
        
        // Test TF computation
        assertDoesNotThrow(() -> {
            searchEngine.computeTF();
        }, "TF computation should not throw exceptions");
    }
    
    @Test
    public void testIDFComputation() {
        // First build the positional index
        searchEngine.buildPositionalIndex();
        
        // Test IDF computation
        assertDoesNotThrow(() -> {
            searchEngine.computeIDF();
        }, "IDF computation should not throw exceptions");
    }
    
    @Test
    public void testTFIDFComputation() {
        // Build dependencies
        searchEngine.buildPositionalIndex();
        searchEngine.computeTF();
        searchEngine.computeIDF();
        
        // Test TF-IDF computation
        assertDoesNotThrow(() -> {
            searchEngine.computeTFIDF();
        }, "TF-IDF computation should not throw exceptions");
    }
    
    @Test
    public void testPhraseQuery() {
        // Build all dependencies
        searchEngine.buildPositionalIndex();
        searchEngine.computeTF();
        searchEngine.computeIDF();
        searchEngine.computeTFIDF();
        
        // Test phrase query processing
        assertDoesNotThrow(() -> {
            searchEngine.processPhraseQuery("information retrieval");
        }, "Phrase query processing should not throw exceptions");
    }
    
    @Test
    public void testComplexQuery() {
        // Build all dependencies
        searchEngine.buildPositionalIndex();
        searchEngine.computeTF();
        searchEngine.computeIDF();
        searchEngine.computeTFIDF();
        
        // Test complex query processing
        assertDoesNotThrow(() -> {
            searchEngine.processPhraseQuery("\"machine learning\" AND \"artificial intelligence\"");
        }, "Complex query processing should not throw exceptions");
    }
}