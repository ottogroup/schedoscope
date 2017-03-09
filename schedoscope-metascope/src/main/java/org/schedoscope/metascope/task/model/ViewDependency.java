package org.schedoscope.metascope.task.model;

public class ViewDependency {

    private String successor;
    private String dependency;

    public ViewDependency(String successor, String dependency) {
        this.successor = successor;
        this.dependency = dependency;
    }

    public String getSuccessor() {
        return successor;
    }

    public void setSuccessor(String successor) {
        this.successor = successor;
    }

    public String getDependency() {
        return dependency;
    }

    public void setDependency(String dependency) {
        this.dependency = dependency;
    }

}
