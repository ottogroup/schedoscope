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

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.schedoscope.metascope.SpringTest;
import org.schedoscope.metascope.model.Metadata;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MetadataEntityServiceTest extends SpringTest {

  private static final String METADATA_TEST_KEY = "testkey";
  private static final String METADATA_TEST_VALUE = "testvalue";
  private static final String NON_EXIST_METADATA_KEY = "nonExistKey";

  @Before
  public void setup() {
    Metadata metadata = new Metadata();
    metadata.setMetadataKey(METADATA_TEST_KEY);
    metadata.setMetadataValue(METADATA_TEST_VALUE);
    metadataEntityRepository.save(metadata);
  }

  @Test
  public void metadataService_01_getMetadataValue() {
    String metadataValue = metadataEntityService.getMetadataValue(METADATA_TEST_KEY);

    assertEquals(metadataValue, METADATA_TEST_VALUE);
  }

  @Test
  public void metadataService_02_getMetadataValue_nonExist() {
    String metadataValue = metadataEntityService.getMetadataValue(NON_EXIST_METADATA_KEY);

    assertTrue(metadataValue == null);
  }

}
