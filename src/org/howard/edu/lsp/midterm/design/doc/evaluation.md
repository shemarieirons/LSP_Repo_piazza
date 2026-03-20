# Design Evaluation OrderProcessor

## Overview

The OrderProcessor class has several design problems. Most of them come from putting too much responsibility into one place and not properly controlling access to data.

## Issue 1: Public Fields

All the fields (customerName, email, item, and price) are public. This means any other part of the program can change them directly without any checks. That makes it hard to control the state of the object. For example, nothing prevents someone from setting a negative price.

In good object-oriented design, data should usually be private and accessed through methods so that rules can be enforced.

## Issue 2: Too Many Responsibilities

The processOrder method is doing a lot of different things at once. It calculates tax, prints a receipt, writes to a file, sends an email, and logs a timestamp.

These are all separate concerns. If any one of them changes, the entire method has to be updated and retested. This makes the class harder to maintain and goes against the idea that a class should have one clear responsibility.

## Issue 3: Incorrect Order of Operations

The discount is applied after the receipt is printed and after the order is written to the file. This means the output is actually wrong because it does not reflect the final price after the discount.

This is not just a design issue but also a functional bug caused by the way everything is combined into one method.

## Issue 4: Mixing Different Types of Logic

The class mixes business logic (like calculating tax and discounts) with system-level tasks (like file writing and sending emails). This makes testing harder because you cannot test the calculations without also dealing with file operations or other side effects.

Separating these concerns would make the system easier to test and modify.

## Issue 5: Missing a Clear Order Object

There is no separate Order class. Instead, the data is stored directly inside OrderProcessor. This makes it harder to reuse the data or pass it around the system.

Creating an Order class would group the data together and make the design cleaner.

## Summary

Overall, the class tries to do too much in one place. It exposes its data, mixes unrelated responsibilities, and even produces incorrect results because of how the logic is ordered.

A better design would separate the data, pricing logic, storage, and communication into different classes, each with a clear purpose.