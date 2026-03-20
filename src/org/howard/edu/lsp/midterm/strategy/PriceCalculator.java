package org.howard.edu.lsp.midterm.strategy;

/**
 * Calculates the final price for a customer purchase using a {@link PricingStrategy}.
 * The strategy is supplied at construction time, allowing the pricing behavior
 * to be swapped independently of the calculator itself.
 *
 * <p>This replaces the original conditional chain with a clean delegation to
 * whichever strategy is provided, making it easy to add new customer types
 * without modifying this class.</p>
 *
 * @author Student
 */
public class PriceCalculator {

    /** The pricing strategy used to compute the final price. */
    private PricingStrategy strategy;

    /**
     * Constructs a PriceCalculator with the given pricing strategy.
     *
     * @param strategy the {@link PricingStrategy} to apply when calculating prices
     */
    public PriceCalculator(PricingStrategy strategy) {
        this.strategy = strategy;
    }

    /**
     * Sets a new pricing strategy, replacing the current one.
     *
     * @param strategy the new {@link PricingStrategy} to use
     */
    public void setStrategy(PricingStrategy strategy) {
        this.strategy = strategy;
    }

    /**
     * Calculates the final price by delegating to the current pricing strategy.
     *
     * @param price the original base price before discounts
     * @return the final price after the strategy's discount is applied
     */
    public double calculatePrice(double price) {
        return strategy.calculatePrice(price);
    }
}
