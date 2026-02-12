/**
 * Name: Shemarie Irons
 */

package org.howard.edu.lsp.assignment3;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Represents a product with its attributes and transformation logic.
 * This class encapsulates product data and applies business rules for pricing and categorization.
 */
public class Product {
    private int productId;
    private String name;
    private BigDecimal price;
    private String category;
    private String priceRange;

    /**
     * Constructs a Product with the specified attributes.
     *
     * @param productId the unique identifier for the product
     * @param name the name of the product
     * @param price the price of the product
     * @param category the category of the product
     */
    public Product(int productId, String name, BigDecimal price, String category) {
        this.productId = productId;
        this.name = name;
        this.price = price;
        this.category = category;
        this.priceRange = "";
    }

    /**
     * Applies all business transformations to this product:
     * - Applies 10% discount for Electronics
     * - Rounds price to 2 decimal places
     * - Upgrades to Premium Electronics if applicable
     * - Determines price range
     */
    public void applyTransformations() {
        // Track original category for discount logic
        boolean wasElectronics = category.equals("Electronics");

        // Apply 10% discount for electronics
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
        priceRange = determinePriceRange();
    }

    /**
     * Determines the price range category based on the product price.
     *
     * @return the price range classification (Low, Medium, High, or Premium)
     */
    private String determinePriceRange() {
        if (price.compareTo(new BigDecimal("10.00")) <= 0) {
            return "Low";
        } else if (price.compareTo(new BigDecimal("100.00")) <= 0) {
            return "Medium";
        } else if (price.compareTo(new BigDecimal("500.00")) <= 0) {
            return "High";
        } else {
            return "Premium";
        }
    }

    /**
     * Converts the product to a CSV-formatted string for output.
     *
     * @return CSV representation of the product
     */
    public String toCsvRow() {
        return productId + "," + name + "," + price + "," + category + "," + priceRange;
    }
}