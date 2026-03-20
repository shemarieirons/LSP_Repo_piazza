# Design Evaluation PriceCalculator

## Current Design Problems

### 1. Open Closed Principle Violation

The calculatePrice method relies on a chain of if statements based on customerType. This means that every time a new type is introduced, the method has to be edited. Because of this, the class is constantly being modified instead of being stable. Over time, this would make the method longer and harder to manage.

### 2. Hard to Extend

There is no clean way to add a new pricing rule without going back into the same method and adding more logic. This becomes a problem in larger systems where multiple developers may need to add new behaviors. Everyone would be forced to modify the same piece of code.

### 3. Responsibilities Are Mixed Together

Each pricing rule represents a different business idea, but they are all placed inside one method. If one rule changes, the entire method has to be reviewed and retested. Separating each rule into its own class would make the system easier to maintain and understand.

### 4. Difficult to Test

Since all logic is inside one method, it is not possible to test each pricing rule independently. You have to rely on passing in specific string values to trigger each case, which is fragile. If the strings change, multiple parts of the system can break at once.

## Summary

The current design depends heavily on conditional logic, which makes it harder to maintain and extend. Using the Strategy Pattern improves this by moving each pricing rule into its own class. This makes the system easier to expand and allows new pricing strategies to be added without changing existing code.