package org.howard.edu.lsp.midterm.strategy;

/**
 * Pricing strategy for regular customers.
 * No discount is applied; the final price equals the base price.
 *
 * @author Student
 */
public class RegularPricingStrategy implements PricingStrategy {

    /**
     * Returns the original price unchanged, as regular customers receive no discount.
     *
     * @param price the original base price
     * @return the base price with no discount applied
     */
    @Override
    public double calculatePrice(double price) {
        return price;
    }
}
