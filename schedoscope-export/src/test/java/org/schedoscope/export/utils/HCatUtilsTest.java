/**
 * Copyright 2016 Otto (GmbH & Co KG)
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

package org.schedoscope.export.utils;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class HCatUtilsTest {

	private Set<String> anonFields = ImmutableSet.copyOf(new String[] { "name",
			"id" });

	private Set<String> anonFieldsEmpty = ImmutableSet.copyOf(new String[] {});

	private String salt = "vD75MqvaasIlCf7H";

	@Test
	public void testHashIfInList() {

		assertEquals("cbcc359878fbe2238c064a6caa240370",
				HCatUtils.getHashValueIfInList("id", "abcd", anonFields, salt));
		assertEquals("not hashed", HCatUtils.getHashValueIfInList("no_id",
				"not hashed", anonFields, salt));
	}

	@Test
	public void testEmptyAnonFields() {
		assertEquals("not hashed", HCatUtils.getHashValueIfInList("no_id",
				"not hashed", anonFieldsEmpty, salt));
	}
}
