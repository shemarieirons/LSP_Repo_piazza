package org.howard.edu.lsp.finalexam.question2;

/**
 * Abstract base class that defines the template method for generating reports.
 */
public abstract class Report {

    /**
     * Template method that defines the fixed report generation workflow.
     */
    public final void generateReport() {
        loadData();
        System.out.println(formatHeader());
        System.out.println(formatBody());
        System.out.println(formatFooter());
    }

    /** Loads data needed for the report. */
    protected abstract void loadData();

    /** Returns the formatted header. */
    protected abstract String formatHeader();

    /** Returns the formatted body. */
    protected abstract String formatBody();

    /** Returns the formatted footer. */
    protected abstract String formatFooter();
}