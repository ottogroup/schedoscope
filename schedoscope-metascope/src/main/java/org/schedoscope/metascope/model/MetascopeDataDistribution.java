package org.schedoscope.metascope.model;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class MetascopeDataDistribution {

  @Id
  private String id;
  private String fqdn;
  private String metric;
  private String value;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getFqdn() {
    return fqdn;
  }

  public void setFqdn(String fqdn) {
    this.fqdn = fqdn;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

}
