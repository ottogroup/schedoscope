package org.schedoscope.metascope.util;

import org.springframework.stereotype.Component;

@Component
public class TaskMutex {

  private boolean schedoscopeTaskRunning;

  public TaskMutex() {
    this.schedoscopeTaskRunning = false;
  }

  public boolean isSchedoscopeTaskRunning() {
    return schedoscopeTaskRunning;
  }

  public void setSchedoscopeTaskRunning(boolean schedoscopeTaskRunning) {
    this.schedoscopeTaskRunning = schedoscopeTaskRunning;
  }

}
