# Assignment 3 Reflection
Shemarie Irons  
LSP

## Overview

In Assignment 2, I implemented an ETL pipeline using a single class that contained all logic inside the main method. While the program worked correctly and met the requirements, the design was procedural and centralized. File reading, validation, transformation, and writing were all handled in one place.

In Assignment 3, I redesigned the solution to be more object-oriented. The program still produces the exact same outputs and handles the same edge cases, but the responsibilities are now separated across multiple classes with clearer roles.

---

## Design Differences Between Assignment 2 and Assignment 3

The most significant difference is the decomposition of responsibilities.

In Assignment 2:
- All logic was inside one class.
- The main method performed parsing, validation, transformation, and file writing.
- The design was linear and procedural.

In Assignment 3:
- A `Product` class represents a single product record.
- A `ProductParser` class handles parsing and validation of CSV rows.
- An `ETLProcessor` class coordinates file reading, transformation, and writing.
- The `ETLPipeline` class serves as the entry point and delegates execution.

This separation improves readability and maintainability. Each class now has a clear, focused responsibility instead of one class doing everything.

---

## How Assignment 3 Is More Object-Oriented

Assignment 3 applies several object-oriented concepts more intentionally.

### Object

Each valid row from the CSV file is represented as a `Product` object. Instead of treating rows as arrays of strings, the program now models each product as an object with meaningful state and behavior.

### Class

The logic is divided into multiple classes:
- `Product` encapsulates product data and transformation logic.
- `ProductParser` handles validation and object creation.
- `ETLProcessor` manages the ETL workflow.

This demonstrates object-oriented decomposition by assigning responsibilities to appropriate classes.

### Encapsulation

The fields in the `Product` class are private. Access and transformation of the data occur through methods rather than direct field manipulation. This ensures that transformation logic such as discount application and price range calculation is controlled within the object itself.

### Polymorphism

Polymorphism is demonstrated through method behavior based on object state. For example, transformation logic behaves differently depending on whether the product category is Electronics. The behavior is determined by the object's state rather than external conditional logic scattered throughout the program.

### Inheritance

Inheritance was considered but ultimately not necessary for this problem. The design remained simple and focused. Since there was only one product type with conditional logic, introducing inheritance would have overcomplicated the solution without improving clarity.

---

## Testing and Verification

To ensure that Assignment 3 behaves exactly like Assignment 2, I tested both implementations using the same input files:

1. The provided sample file with invalid rows.
2. An empty input file containing only the header.
3. A missing input file scenario.
4. A high-value Electronics product to trigger Premium Electronics.
5. Boundary price cases for price range validation.

For each case, I compared the generated `data/transformed_products.csv` file from Assignment 2 and Assignment 3. The outputs matched exactly in content, formatting, rounding behavior, and error handling. I also verified that row counts and summary output were consistent.

This confirmed that Assignment 3 preserves the required behavior while improving the design structure.

---

## Use of Generative AI

I used a generative AI assistant to brainstorm how to refactor a procedural ETL pipeline into a more object-oriented design. The AI suggested separating responsibilities into parsing, transformation, and orchestration classes. It also suggested moving transformation logic into the `Product` class to better demonstrate encapsulation.

I reviewed and adapted the suggestions to ensure that:
- The program behavior remained identical to Assignment 2.
- The design was not overengineered.
- The implementation followed lecture coding standards.

I also used AI assistance to help generate initial Javadoc templates, which I reviewed and edited for correctness and clarity.

Overall, AI was used as a brainstorming and drafting tool, but all design decisions and final edits were carefully reviewed and implemented by me.