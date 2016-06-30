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
package org.schedoscope.metascope.tasks;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.schedoscope.metascope.SpringTest;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.model.ViewEntity;
import org.schedoscope.metascope.util.SchedoscopeConnectException;
import org.schedoscope.metascope.util.SchedoscopeUtil;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class SchedoscopeSyncRepositoryTest extends SpringTest {

	private static final int TABLES_TESTDATA_COUNT = 8;
	private static final int VIEWS_TESTDATA_COUNT = 35;
	private static final int FIELDS_TESTDATA_COUNT = 55;
	private static final int TABLEDEPENDENCIES_TESTDATA_COUNT = 9;
	private static final int TRANSFORMATION_TESTDATA_COUNT = 13;
	private static final int PARAMETERVALUE_TESTDATA_COUNT = 84;
	private static final int DEPENDENCY_TESTDATA_COUNT = 144;

	private SchedoscopeSyncTask schedoscopeSyncTask;

	@Before
	public void setupLocal() throws IOException, SchedoscopeConnectException {
		SolrFacade solrFacadeMock = mock(SolrFacade.class);
		SchedoscopeUtil schedoscopeUtilMock = mock(SchedoscopeUtil.class);

		String json = Resources.toString(
				Resources.getResource("schedoscope-rest.json"), Charsets.UTF_8);

		when(
				schedoscopeUtilMock
						.getViewsAsJsonFromSchedoscope(any(Boolean.class)))
				.thenReturn(json);
		when(schedoscopeUtilMock.getViewStatus(any(Boolean.class)))
				.thenCallRealMethod();
		when(schedoscopeUtilMock.getViewStatusFromJson(any(String.class)))
				.thenCallRealMethod();
		when(
				schedoscopeUtilMock.getStatus(Matchers
						.anyListOf(ViewEntity.class))).thenCallRealMethod();

		this.schedoscopeSyncTask = new SchedoscopeSyncTask(repo, dataSource,
				solrFacadeMock, null, schedoscopeUtilMock);
	}

	@Test
	public void schedoscopeSyncTask_01_run() {
		assertEquals(size(tableEntityRepository.findAll()), 0);
		assertEquals(size(viewEntityRepository.findAll()), 0);
		assertEquals(size(fieldEntityRepository.findAll()), 0);
		assertEquals(size(tableDependencyEntityRepository.findAll()), 0);
		assertEquals(size(transformationEntityRepository.findAll()), 0);
		assertEquals(size(parameterValueEntityRepository.findAll()), 0);
		assertEquals(size(viewDependencyEntityRepository.findAll()), 0);
		assertEquals(size(successorEntityRepository.findAll()), 0);

		Connection connection;
		try {
			connection = dataSource.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
			return;
		}

		schedoscopeSyncTask.run(System.currentTimeMillis());

		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		assertEquals(size(tableEntityRepository.findAll()),
				TABLES_TESTDATA_COUNT);
		assertEquals(size(viewEntityRepository.findAll()), VIEWS_TESTDATA_COUNT);
		assertEquals(size(fieldEntityRepository.findAll()),
				FIELDS_TESTDATA_COUNT);
		assertEquals(size(tableDependencyEntityRepository.findAll()),
				TABLEDEPENDENCIES_TESTDATA_COUNT);
		assertEquals(size(transformationEntityRepository.findAll()),
				TRANSFORMATION_TESTDATA_COUNT);
		assertEquals(size(parameterValueEntityRepository.findAll()),
				PARAMETERVALUE_TESTDATA_COUNT);
		assertEquals(size(viewDependencyEntityRepository.findAll()),
				DEPENDENCY_TESTDATA_COUNT);
		assertEquals(size(successorEntityRepository.findAll()),
				DEPENDENCY_TESTDATA_COUNT);
	}

}
