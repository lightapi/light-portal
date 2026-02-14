package net.lightapi.portal.validation;

/**
 * Represents a sorting criterion for database queries.
 */
public class SortCriterion {
    String id;
    boolean desc;

    /**
     * Constructs a SortCriterion.
     *
     * @param id   The field ID to sort by.
     * @param desc True for descending order, false for ascending.
     */
    public SortCriterion(String id, boolean desc) {
        this.id = id;
        this.desc = desc;
    }
    /**
     * Gets the sort field ID.
     *
     * @return The field ID.
     */
    public String getId() {
        return id;
    }
    /**
     * Sets the sort field ID.
     *
     * @param id The field ID.
     */
    public void setId(String id) {
        this.id = id;
    }
    /**
     * Checks if sorting is descending.
     *
     * @return True if descending, false if ascending.
     */
    public boolean isDesc() {
        return desc;
    }
    /**
     * Sets the sort order.
     *
     * @param desc True for descending, false for ascending.
     */
    public void setDesc(boolean desc) {
        this.desc = desc;
    }
}
