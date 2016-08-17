/**
 * Copyright 2015 Otto (GmbH & Co KG)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.metascope;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.schedoscope.metascope.controller.TableEntityControllerTest;
import org.schedoscope.metascope.index.SolrFacadeTest;
import org.schedoscope.metascope.service.*;
import org.schedoscope.metascope.tasks.SchedoscopeSyncRepositoryTest;

@RunWith(Suite.class)
@SuiteClasses({SchedoscopeSyncRepositoryTest.class, UserEntityServiceTest.class, TaxonomyServiceTest.class,
        TableEntityServiceTest.class, ActivityEntityServiceTest.class, DocumentationServiceTest.class,
        FieldEntityServiceTest.class, MetadataEntityServiceTest.class, URLServiceTest.class, SolrFacadeTest.class,
        TableEntityControllerTest.class})
public class MetascopeTestSuit {
}
