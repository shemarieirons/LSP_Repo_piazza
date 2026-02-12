/**
 * Name: Shemarie Irons
 */
package org.howard.edu.lsp.assignment3;

import java.math.BigDecimal;

/**
 * Responsible for parsing CSV lines into Product objects.
 * Handles validation and returns null for invalid input.
 */
public class ProductParser {

    /**
     * Parses a CSV line into a Product object.
     * Validates field count and numeric data.
     *
     * @param csvLine the CSV line to parse
     * @return a Product object if parsing succeeds, null otherwise
     */
    public Product parse(String csvLine) {
        if (csvLine == null || csvLine.trim().isEmpty()) {
            return null;
        }

        String[] fields = csvLine.split(",");

        // Validate correct number of fields
        if (fields.length != 4) {
            return null;
        }

        try {
            int productId = Integer.parseInt(fields[0].trim());
            String name = fields[1].trim().toUpperCase();
            BigDecimal price = new BigDecimal(fields[2].trim());
            String category = fields[3].trim();

            return new Product(productId, name, price, category);
        } catch (NumberFormatException e) {
            // Return null for invalid numeric fields
            return null;
        }
    }
}
