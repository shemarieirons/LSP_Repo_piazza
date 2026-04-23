AI Tools Used:  
ChatGPT

Prompts Used (2–5 max):
1. Generate Javadoc comments for this code:
   package org.howard.edu.lsp.finalexam.question2;

public abstract class Report {

    public final void generateReport() {
        loadData();
        System.out.println(formatHeader());
        System.out.println(formatBody());
        System.out.println(formatFooter());
    }

    protected abstract void loadData();
    protected abstract String formatHeader();
    protected abstract String formatBody();
    protected abstract String formatFooter();
}

2. “What is the role of the abstract class in the Template Method pattern?”
3. “Check if my workflow order is correct.”

How AI Helped (2–3 sentences):  
AI was mainly used to generate Javadoc comments and to verify small design details. It helped confirm that my method structure followed the expected workflow. 

Reflection (1–2 sentences):  
I reinforced my understanding of how the Template Method pattern structures program flow. I also improved my ability to properly document code using Javadocs.