/**
 * Name: Shemarie Irons
 */
package org.howard.edu.lsp.assignment2;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class ETLPipeline {

    public static void main(String[] args) {

        // Relative file paths for input and output
        String inputPath = "data/products.csv";
        String outputPath = "data/transformed_products.csv";

        // Counters for summary output
        int rowsRead = 0;
        int rowsWritten = 0;
        int rowsSkipped = 0;

        // Check if input file exists
        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            System.out.println("Error: Input file not found at " + inputPath);
            return;
        }

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

            // Read input file line by line
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

                // Split CSV fields
                String[] fields = line.split(",");

                // Validate correct number of fields
                if (fields.length != 4) {
                    rowsSkipped++;
                    continue;
                }

                try {
                    // Parse and clean fields
                    int productId = Integer.parseInt(fields[0].trim());
                    String name = fields[1].trim().toUpperCase();
                    BigDecimal price = new BigDecimal(fields[2].trim());
                    String category = fields[3].trim();

                    // Track original category
                    boolean wasElectronics = category.equals("Electronics");

                    // Apply discount for electronics
                    if (wasElectronics) {
                        price = price.multiply(new BigDecimal("0.90"));
                    }

                    // Round price to two decimal places
                    price = price.setScale(2, RoundingMode.HALF_UP);

                    // Update category for premium electronics
                    if (wasElectronics && price.compareTo(new BigDecimal("500.00")) > 0) {
                        category = "Premium Electronics";
                    }

                    // Assign price range
                    String priceRange;
                    if (price.compareTo(new BigDecimal("10.00")) <= 0) {
                        priceRange = "Low";
                    } else if (price.compareTo(new BigDecimal("100.00")) <= 0) {
                        priceRange = "Medium";
                    } else if (price.compareTo(new BigDecimal("500.00")) <= 0) {
                        priceRange = "High";
                    } else {
                        priceRange = "Premium";
                    }

                    // Write transformed row to output file
                    writer.write(productId + "," + name + "," + price + "," + category + "," + priceRange);
                    writer.newLine();
                    rowsWritten++;

                    // Handle invalid numeric fields
                } catch (NumberFormatException e) {
                    rowsSkipped++;
                }
            }

            // Print execution summary
            System.out.println("Rows read: " + rowsRead);
            System.out.println("Rows transformed: " + rowsWritten);
            System.out.println("Rows skipped: " + rowsSkipped);
            System.out.println("Output written to: " + outputPath);

            // Handle file access errors
        } catch (IOException e) {
            System.out.println("Error processing files: " + e.getMessage());
        }
    }
}
