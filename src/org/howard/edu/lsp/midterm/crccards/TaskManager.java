package org.howard.edu.lsp.midterm.crccards;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages a collection of {@link Task} objects in the Task Management System.
 * Supports adding tasks, finding tasks by ID, and retrieving tasks filtered by status.
 * Internally uses a {@link LinkedHashMap} keyed by task ID to prevent duplicates
 * and support efficient lookup.
 *
 * @author Student
 */
public class TaskManager {

    /** Internal storage: maps each taskId to its corresponding Task object. */
    private Map<String, Task> tasks;

    /**
     * Constructs a new, empty TaskManager.
     */
    public TaskManager() {
        tasks = new LinkedHashMap<>();
    }

    /**
     * Adds a task to the manager.
     * If a task with the same task ID already exists, an exception is thrown.
     *
     * @param task the Task to add
     * @throws IllegalArgumentException if a task with the same ID is already present
     */
    public void addTask(Task task) {
        if (tasks.containsKey(task.getTaskId())) {
            throw new IllegalArgumentException(
                    "Task with ID " + task.getTaskId() + " already exists.");
        }
        tasks.put(task.getTaskId(), task);
    }

    /**
     * Finds and returns the task with the specified ID.
     * Returns {@code null} if no task with that ID is found.
     *
     * @param taskId the ID of the task to locate
     * @return the matching {@link Task}, or {@code null} if not found
     */
    public Task findTask(String taskId) {
        return tasks.getOrDefault(taskId, null);
    }

    /**
     * Returns a list of all tasks whose status matches the specified value.
     * The comparison is case-sensitive.
     *
     * @param status the status to filter by (e.g., "OPEN", "IN_PROGRESS", "COMPLETE")
     * @return a {@link List} of tasks with the given status; may be empty if none match
     */
    public List<Task> getTasksByStatus(String status) {
        List<Task> result = new ArrayList<>();
        for (Task task : tasks.values()) {
            if (task.getStatus().equals(status)) {
                result.add(task);
            }
        }
        return result;
    }
}
