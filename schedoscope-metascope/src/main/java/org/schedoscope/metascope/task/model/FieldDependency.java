package org.schedoscope.metascope.task.model;

public class FieldDependency {

  private String successor;
  private String dependency;

  public FieldDependency(String successor, String dependency) {
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
