package org.howard.edu.lsp.midterm.strategy;

/**
 * Pricing strategy for VIP customers.
 * VIP customers receive a 20% discount on their purchase.
 *
 * @author Student
 */
public class VIPPricingStrategy implements PricingStrategy {

    /**
     * Applies a 20% discount to the base price.
     *
     * @param price the original base price
     * @return the price after a 20% discount
     */
    @Override
    public double calculatePrice(double price) {
        return price * 0.80;
    }
}
