package net.lightapi.portal.validation;

public class SortCriterion {
    String id;
    boolean desc;

    public SortCriterion(String id, boolean desc) {
        this.id = id;
        this.desc = desc;
    }
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public boolean isDesc() {
        return desc;
    }
    public void setDesc(boolean desc) {
        this.desc = desc;
    }
}
