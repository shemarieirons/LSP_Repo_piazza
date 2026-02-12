/**
 * Name: Shemarie Irons
 */
package org.howard.edu.lsp.assignment3;

import java.io.*;

/**
 * Coordinates the ETL (Extract, Transform, Load) pipeline.
 * Responsible for reading input, processing data, writing output, and tracking statistics.
 */
public class ETLProcessor {
    private String inputPath;
    private String outputPath;
    private int rowsRead;
    private int rowsWritten;
    private int rowsSkipped;

    /**
     * Constructs an ETLProcessor with the specified input and output file paths.
     *
     * @param inputPath the path to the input CSV file
     * @param outputPath the path to the output CSV file
     */
    public ETLProcessor(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.rowsRead = 0;
        this.rowsWritten = 0;
        this.rowsSkipped = 0;
    }

    /**
     * Executes the ETL pipeline process.
     * Reads the input file, transforms data, and writes to output file.
     */
    public void run() {
        // Check if input file exists
        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            System.out.println("Error: Input file not found at " + inputPath);
            return;
        }

        ProductParser parser = new ProductParser();

        // Try with resources to handle file reading and writing
        try (
                BufferedReader reader = new BufferedReader(new FileReader(inputPath));
                BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))
        ) {
            String line;
            boolean isHeader = true;

            // Write header to output file
            writer.write("ProductID,Name,Price,Category,PriceRange");
            writer.newLine();

            // Read and process input file line by line
            while ((line = reader.readLine()) != null) {
                // Skip the input header row
                if (isHeader) {
                    isHeader = false;
                    continue;
                }

                // Handle blank lines
                if (line.trim().isEmpty()) {
                    rowsRead++;
                    rowsSkipped++;
                    continue;
                }

                rowsRead++;

                // Parse the CSV line into a Product object
                Product product = parser.parse(line);

                // Skip invalid rows
                if (product == null) {
                    rowsSkipped++;
                    continue;
                }

                // Apply transformations to the product
                product.applyTransformations();

                // Write transformed product to output file
                writer.write(product.toCsvRow());
                writer.newLine();
                rowsWritten++;
            }

            // Print execution summary
            printSummary();

        } catch (IOException e) {
            System.out.println("Error processing files: " + e.getMessage());
        }
    }

    /**
     * Prints a summary of the ETL execution statistics.
     */
    private void printSummary() {
        System.out.println("Rows read: " + rowsRead);
        System.out.println("Rows transformed: " + rowsWritten);
        System.out.println("Rows skipped: " + rowsSkipped);
        System.out.println("Output written to: " + outputPath);
    }
}