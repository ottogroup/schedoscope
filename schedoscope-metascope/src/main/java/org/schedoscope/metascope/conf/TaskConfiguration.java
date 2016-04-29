/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.metascope.conf;

import java.util.concurrent.Executor;

import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.aop.interceptor.SimpleAsyncUncaughtExceptionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class TaskConfiguration implements AsyncConfigurer {

  @Bean
  public TaskExecutor taskExecutor() {
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    threadPoolTaskExecutor.setCorePoolSize(6);
    threadPoolTaskExecutor.setMaxPoolSize(6);
    threadPoolTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
    return threadPoolTaskExecutor;
  }
  
  @Bean
  public TaskExecutor asyncTaskExecutor() {
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    threadPoolTaskExecutor.setCorePoolSize(10);
    threadPoolTaskExecutor.setMaxPoolSize(10);
    threadPoolTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
    return threadPoolTaskExecutor;
  }

	@Override
  public Executor getAsyncExecutor() {
    return asyncTaskExecutor();
  }

	@Override
  public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
	  return new SimpleAsyncUncaughtExceptionHandler();
  }

}
