package org.howard.edu.lsp.midterm.crccards;

/**
 * Represents a single task in the Task Management System.
 * Stores task information including an ID, description, and status,
 * and provides methods to read and update that information.
 *
 * @author Student
 */
public class Task {

    private String taskId;
    private String description;
    private String status;

    /**
     * Constructs a new Task with the given ID and description.
     * The status is set to "OPEN" by default.
     *
     * @param taskId      the unique identifier for this task
     * @param description a brief description of the task
     */
    public Task(String taskId, String description) {
        this.taskId = taskId;
        this.description = description;
        this.status = "OPEN";
    }

    /**
     * Returns the unique ID of this task.
     *
     * @return the task ID
     */
    public String getTaskId() {
        return taskId;
    }

    /**
     * Returns the description of this task.
     *
     * @return the task description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Returns the current status of this task.
     *
     * @return the task status
     */
    public String getStatus() {
        return status;
    }

    /**
     * Sets the status of this task.
     * Valid values are "OPEN", "IN_PROGRESS", and "COMPLETE" (case-sensitive).
     * Any other value will cause the status to be set to "UNKNOWN".
     *
     * @param status the new status to assign to this task
     */
    public void setStatus(String status) {
        if (status.equals("OPEN") || status.equals("IN_PROGRESS") || status.equals("COMPLETE")) {
            this.status = status;
        } else {
            this.status = "UNKNOWN";
        }
    }

    /**
     * Returns a string representation of this task in the format:
     * {@code taskId description [status]}
     *
     * @return formatted string describing this task
     */
    @Override
    public String toString() {
        return taskId + " " + description + " [" + status + "]";
    }
}
