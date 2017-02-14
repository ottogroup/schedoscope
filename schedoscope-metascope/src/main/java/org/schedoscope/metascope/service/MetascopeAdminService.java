package org.schedoscope.metascope.service;

import org.schedoscope.metascope.task.MetascopeTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


@Service
public class MetascopeAdminService {

  @Autowired
  private MetascopeTask metascopeTask;

  @Async
  @Transactional
  public void schedule() {
    metascopeTask.run();
  }

}
