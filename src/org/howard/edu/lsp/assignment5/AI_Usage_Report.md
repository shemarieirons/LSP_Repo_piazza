# AI Usage Report – IntegerSet Implementation

**Student:** Shemarie Irons
 
---

## Prompt 1 – Instance Variables and Constructor

**Question:** When implementing a Java class called IntegerSet that represents a mathematical set of integers using ArrayList, what instance variables should the class have? How should I initialize the list in the constructor?

**Summary:** The AI suggested using a single private `ArrayList<Integer>` as the only instance variable to store the set's elements. The constructor should initialize it as an empty ArrayList. No additional variables like a separate size counter are needed because ArrayList tracks its own size automatically.
 
---

## Prompt 2 – Preventing Duplicates in `add()`

**Question:** In the IntegerSet class, the `add(int item)` method should not allow duplicates. How do I check before adding an item?

**Summary:** The AI explained that duplicates can be prevented by calling `contains()` before adding. If the item is already present, it is simply skipped. The AI also noted that a manual loop through the list would work as an alternative if `contains()` were not permitted.
 
---

## Prompt 3 – Removing a Specific Integer Value

**Question:** How do I remove a specific integer value from an ArrayList without accidentally removing the wrong element?

**Summary:** The AI clarified that ArrayList has two overloaded `remove` methods — one that removes by index and one that removes by object value. To safely remove an integer value rather than an element at a position, the item must be wrapped using `Integer.valueOf()` so Java resolves to the correct overload. The AI also suggested optionally checking with `contains()` first for additional safety.
 
---

## Prompt 4 – Finding Largest and Smallest, and Handling Empty Sets

**Question:** How do I find the largest and smallest values in the set, and what should happen if the set is empty?

**Summary:** The AI recommended using `Collections.max()` and `Collections.min()` as the simplest approach. For the empty set case, the AI advised throwing a `NoSuchElementException` with a descriptive message, since a mathematical set with no elements has no defined largest or smallest value. Manually iterating through the list was also mentioned as a valid alternative.
 
---

## Prompt 5 – Edge Cases and JUnit Tests

**Question:** What are some edge cases to test for the IntegerSet implementation covering union, intersection, difference, and complement? Can you provide JUnit test examples that also verify operations do not modify the original sets?

**Summary:** The AI identified the following key edge cases to cover: empty sets, identical sets, disjoint sets, partial overlap, single-element sets, negative numbers, and duplicate prevention. JUnit 5 test examples were provided for union, intersection, difference, and complement, with dedicated tests confirming that the original sets remain unmodified after each operation. The AI emphasized using a variety of input scenarios such as overlapping sets and fully empty sets to ensure correctness across all cases.
 
---

## Prompt 6 – Javadoc Documentation

**Question:** Can you provide Javadoc comments for the entire IntegerSet class, including the constructor and all methods?

**Summary:** The AI generated complete Javadoc comments for the class. The class-level comment describes its purpose as a mathematical set of integers. Each method was documented with a description, `@param` tags where applicable, `@return` tags, and `@throws` tags for methods that throw exceptions. The AI specifically noted that `largest()` and `smallest()` should document the `NoSuchElementException`, and that all set operation methods should clarify that they return new sets without modifying the originals.
 
---


Transcript: https://chatgpt.com/share/69cec638-afb8-8328-98a6-1e0daf248f41