package org.howard.edu.lsp.midterm.strategy;

/**
 * Pricing strategy for holiday customers.
 * Holiday customers receive a 15% discount on their purchase.
 *
 * @author Student
 */
public class HolidayPricingStrategy implements PricingStrategy {

    /**
     * Applies a 15% discount to the base price.
     *
     * @param price the original base price
     * @return the price after a 15% discount
     */
    @Override
    public double calculatePrice(double price) {
        return price * 0.85;
    }
}
