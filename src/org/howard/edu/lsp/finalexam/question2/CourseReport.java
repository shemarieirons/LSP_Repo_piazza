package org.howard.edu.lsp.finalexam.question2;

/**
 * Concrete report for a course. Implements the Template Method steps defined in Report.
 */
public class CourseReport extends Report {

    private String courseName;
    private int enrollment;

    /** Sets courseName and enrollment. */
    @Override
    protected void loadData() {
        courseName = "CSCI 363";
        enrollment = 45;
    }

    @Override
    protected String formatHeader() {
        return "=== HEADER ===\nCourse Report";
    }

    @Override
    protected String formatBody() {
        return "=== BODY ===\nCourse: " + courseName + "\nEnrollment: " + enrollment;
    }

    @Override
    protected String formatFooter() {
        return "=== FOOTER ===\nEnd of Course Report";
    }
}