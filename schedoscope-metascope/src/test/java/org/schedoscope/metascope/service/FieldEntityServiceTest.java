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
package org.schedoscope.metascope.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.schedoscope.metascope.SpringTest;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FieldEntityServiceTest extends SpringTest {

	private static final String PARAMETER_YEAR = "year";
	private static final String PARAMETER_MONTH = "month";
	private static final String PARAMETER_MONTH_ID = "month_id";

	@Test
	public void fieldEntityService_01_findDistinctParameters() {
		List<String> distinctParameter = fieldEntityService
				.findDistinctParameters();

		assertTrue(distinctParameter != null);
		assertEquals(distinctParameter.size(), 3);

		for (String parameter : distinctParameter) {
			assertTrue(parameter.equals(PARAMETER_YEAR)
					|| parameter.equals(PARAMETER_MONTH)
					|| parameter.equals(PARAMETER_MONTH_ID));
		}
	}

}
