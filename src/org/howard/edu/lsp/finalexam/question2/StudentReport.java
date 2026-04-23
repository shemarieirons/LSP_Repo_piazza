package org.howard.edu.lsp.finalexam.question2;

/**
 * Concrete report for a student. Implements the Template Method steps defined in Report.
 */
public class StudentReport extends Report {

    private String studentName;
    private double gpa;

    /** Sets studentName and gpa. */
    @Override
    protected void loadData() {
        studentName = "John Doe";
        gpa = 3.8;
    }

    @Override
    protected String formatHeader() {
        return "=== HEADER ===\nStudent Report";
    }

    @Override
    protected String formatBody() {
        return "=== BODY ===\nStudent Name: " + studentName + "\nGPA: " + gpa;
    }

    @Override
    protected String formatFooter() {
        return "=== FOOTER ===\nEnd of Student Report";
    }
}