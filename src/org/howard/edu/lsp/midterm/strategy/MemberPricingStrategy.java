package org.howard.edu.lsp.midterm.strategy;

/**
 * Pricing strategy for member customers.
 * Members receive a 10% discount on their purchase.
 *
 * @author Student
 */
public class MemberPricingStrategy implements PricingStrategy {

    /**
     * Applies a 10% discount to the base price.
     *
     * @param price the original base price
     * @return the price after a 10% discount
     */
    @Override
    public double calculatePrice(double price) {
        return price * 0.90;
    }
}
