package org.howard.edu.lsp.assignment5;

import java.util.ArrayList;
import java.util.Collections;
import java.util.NoSuchElementException;

/**
 * The IntegerSet class represents a mathematical set of integers.
 * It uses an ArrayList internally to store elements while ensuring
 * no duplicates are allowed. Supports standard set operations like
 * union, intersection, difference, and complement.
 */
public class IntegerSet {

    /** Internal storage of set elements. */
    private ArrayList<Integer> set = new ArrayList<>();

    /**
     * Constructs an empty IntegerSet.
     */
    public IntegerSet() {
    }

    /**
     * Clears the set, removing all elements.
     */
    public void clear() {
        set.clear();
    }

    /**
     * Returns the number of elements in the set.
     *
     * @return the size of the set
     */
    public int length() {
        return set.size();
    }

    /**
     * Compares this set with another for equality.
     * Two sets are equal if they contain the same elements,
     * regardless of order.
     *
     * @param b the other IntegerSet to compare with
     * @return true if sets are equal, false otherwise
     */
    public boolean equals(IntegerSet b) {
        if (b == null) return false;
        if (this.length() != b.length()) return false;

        ArrayList<Integer> copy = new ArrayList<>(this.set);
        Collections.sort(copy);

        ArrayList<Integer> bCopy = new ArrayList<>(b.set);
        Collections.sort(bCopy);

        return copy.equals(bCopy);
    }

    /**
     * Checks if a value exists in the set.
     *
     * @param value the integer to check
     * @return true if the value exists, false otherwise
     */
    public boolean contains(int value) {
        return set.contains(value);
    }

    /**
     * Returns the largest element in the set.
     *
     * @return the largest integer in the set
     * @throws NoSuchElementException if the set is empty
     */
    public int largest() {
        if (isEmpty()) throw new NoSuchElementException("Set is empty");
        return Collections.max(set);
    }

    /**
     * Returns the smallest element in the set.
     *
     * @return the smallest integer in the set
     * @throws NoSuchElementException if the set is empty
     */
    public int smallest() {
        if (isEmpty()) throw new NoSuchElementException("Set is empty");
        return Collections.min(set);
    }

    /**
     * Adds an item to the set if it is not already present.
     *
     * @param item the integer to add
     */
    public void add(int item) {
        if (!set.contains(item)) {
            set.add(item);
        }
    }

    /**
     * Removes a specified integer from the set.
     *
     * @param item the integer to remove
     */
    public void remove(int item) {
        set.remove(Integer.valueOf(item));
    }

    /**
     * Returns a new set representing the union of this set and another set.
     * Original sets are not modified.
     *
     * @param intSetb the other IntegerSet
     * @return a new IntegerSet containing all unique elements from both sets
     */
    public IntegerSet union(IntegerSet intSetb) {
        IntegerSet result = new IntegerSet();
        result.set.addAll(this.set);
        for (int val : intSetb.set) {
            if (!result.set.contains(val)) {
                result.set.add(val);
            }
        }
        return result;
    }

    /**
     * Returns a new set representing the intersection of this set and another set.
     * Original sets are not modified.
     *
     * @param intSetb the other IntegerSet
     * @return a new IntegerSet containing elements present in both sets
     */
    public IntegerSet intersect(IntegerSet intSetb) {
        IntegerSet result = new IntegerSet();
        result.set.addAll(this.set);
        result.set.retainAll(intSetb.set);
        return result;
    }

    /**
     * Returns a new set representing the difference between this set and another set.
     * Original sets are not modified.
     *
     * @param intSetb the other IntegerSet
     * @return a new IntegerSet containing elements present in this set but not in intSetb
     */
    public IntegerSet diff(IntegerSet intSetb) {
        IntegerSet result = new IntegerSet();
        result.set.addAll(this.set);
        result.set.removeAll(intSetb.set);
        return result;
    }

    /**
     * Returns a new set representing the complement of this set with respect to another set.
     * Original sets are not modified.
     *
     * @param intSetb the other IntegerSet
     * @return a new IntegerSet containing elements in intSetb but not in this set
     */
    public IntegerSet complement(IntegerSet intSetb) {
        IntegerSet result = new IntegerSet();
        result.set.addAll(intSetb.set);
        result.set.removeAll(this.set);
        return result;
    }

    /**
     * Checks whether the set is empty.
     *
     * @return true if the set has no elements, false otherwise
     */
    public boolean isEmpty() {
        return set.isEmpty();
    }

    /**
     * Returns a string representation of the set.
     * The elements are sorted in ascending order.
     *
     * @return string of the form [element1, element2, ...]
     */
    @Override
    public String toString() {
        ArrayList<Integer> sorted = new ArrayList<>(set);
        Collections.sort(sorted);
        return sorted.toString();
    }
}