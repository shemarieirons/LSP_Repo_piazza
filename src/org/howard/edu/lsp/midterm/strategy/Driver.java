package org.howard.edu.lsp.midterm.strategy;

/**
 * Driver class that demonstrates the Strategy Pattern implementation
 * of {@link PriceCalculator} with four different customer types,
 * each using a distinct pricing strategy.
 *
 * @author Student
 */
public class Driver {

    /**
     * Entry point. Demonstrates REGULAR, MEMBER, VIP, and HOLIDAY pricing
     * strategies applied to a base price of 100.0.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {

        double basePrice = 100.0;

        PriceCalculator calculator = new PriceCalculator(new RegularPricingStrategy());
        System.out.println("REGULAR: " + calculator.calculatePrice(basePrice));

        calculator.setStrategy(new MemberPricingStrategy());
        System.out.println("MEMBER: " + calculator.calculatePrice(basePrice));

        calculator.setStrategy(new VIPPricingStrategy());
        System.out.println("VIP: " + calculator.calculatePrice(basePrice));

        calculator.setStrategy(new HolidayPricingStrategy());
        System.out.println("HOLIDAY: " + calculator.calculatePrice(basePrice));
    }
}
