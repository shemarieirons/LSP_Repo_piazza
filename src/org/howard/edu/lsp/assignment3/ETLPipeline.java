/**
 * Name: Shemarie Irons
 */
package org.howard.edu.lsp.assignment3;

/**
 * Main entry point for the ETL pipeline application.
 * This class simply creates and runs the ETL processor.
 */
public class ETLPipeline {

    /**
     * Main method that starts the ETL pipeline.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        // Relative file paths for input and output
        String inputPath = "data/products.csv";
        String outputPath = "data/transformed_products.csv";

        // Create and run the ETL processor
        ETLProcessor processor = new ETLProcessor(inputPath, outputPath);
        processor.run();
    }
}