# AI Prompts for Assignment 3
Shemarie Irons

Below are selected prompts I used while redesigning Assignment 2 to be more object-oriented.

---

## Prompt 1

My ETL program from Assignment 2 works, but everything is inside main. How do I break this up into multiple classes without changing what it does?

### AI Response Summary

The AI suggested separating the program into different classes with specific responsibilities. For example:
- One class to represent a product
- One class to handle parsing and validation
- One class to control the overall ETL process

I used this idea and created Product, ProductParser, and ETLProcessor classes. This helped organize the program so that each class had one clear job.

---

## Prompt 2

Where should I put the discount and price range calculations in an object-oriented design?

### AI Response Summary

The AI explained that behavior related to a product should live inside the Product class. Instead of calculating discounts in the main pipeline class, I moved that logic into methods inside Product. This made the design cleaner and better organized.

---

## Prompt 3

Do I need inheritance for this assignment? Or is that overcomplicating it?

### AI Response Summary

The AI explained that inheritance is useful when there are multiple related types that share behavior. Since this assignment only works with one type of product, adding inheritance would not improve the design. I decided not to use inheritance to keep the program simple and clear.

---

## Prompt 4

How can I make sure Assignment 3 still works exactly the same as Assignment 2?

### AI Response Summary

The AI suggested running both versions of the program with the same input files and comparing the output files. It also recommended testing edge cases like missing files and empty inputs. I followed this approach to confirm that both versions produce identical results.

---

## Prompt 5

Can you help me write Javadocs for my classes and methods? I want to make sure they sound professional but still match what my code actually does.

### AI Response Summary

The AI generated draft Javadocs. I reviewed them carefully and edited them to make sure they accurately describe my implementation.