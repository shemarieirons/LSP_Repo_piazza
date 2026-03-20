package org.howard.edu.lsp.midterm.strategy;

/**
 * Defines the contract for a pricing strategy.
 * Implementations of this interface encapsulate a single discount rule
 * and compute the final price for a given base price.
 *
 * @author Student
 */
public interface PricingStrategy {

    /**
     * Calculates the final price after applying this strategy's discount rule.
     *
     * @param price the original base price before any discount
     * @return the final price after the discount is applied
     */
    double calculatePrice(double price);
}
