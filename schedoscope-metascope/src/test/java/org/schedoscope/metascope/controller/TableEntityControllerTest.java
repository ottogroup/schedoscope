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
package org.schedoscope.metascope.controller;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.model;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.view;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mockito;
import org.schedoscope.metascope.SpringTest;
import org.schedoscope.metascope.model.TableDependencyEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.service.TableEntityService;
import org.schedoscope.metascope.util.HiveQueryResult;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.annotation.Transactional;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TableEntityControllerTest extends SpringTest {

  private MockMvc mockMvc;

  @Before
  public void setupLocal() {
    this.mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();

    TableEntityService tableEntityServiceMock = Mockito.mock(TableEntityService.class);
    Mockito.when(tableEntityServiceMock.findByFqdn(Mockito.anyString())).thenReturn(getTestTable());
    Mockito.when(tableEntityServiceMock.getSuccessors(Mockito.any(TableEntity.class))).thenReturn(
        new ArrayList<TableDependencyEntity>());
    Mockito.when(
        tableEntityServiceMock.getRequestedViewPage(Mockito.any(TableEntity.class), Mockito.any(Pageable.class)))
        .thenReturn(
            viewEntityRepository.findByFqdnOrderByInternalViewId(getTestTable().getFqdn(), new PageRequest(0, 10)));
    Mockito.when(tableEntityServiceMock.getSample(Mockito.anyString(), Mockito.anyMapOf(String.class, String.class)))
        .thenReturn(new AsyncResult<HiveQueryResult>(new HiveQueryResult("testing")));
    Mockito.when(
        tableEntityServiceMock.runDataDistribution(Mockito.any(TableEntity.class), Mockito.anyString(),
            Mockito.anyInt())).thenReturn(null);
    mockField(tableEntityController, "tableEntityService", tableEntityServiceMock);
  }

  @Test
  @Transactional
  public void tableController_01_getTableDetailPage() throws Exception {
    mockField(tableEntityController, "tableEntityService", tableEntityService);

    mockMvc.perform(get("/table").param("fqdn", getTestTable().getFqdn())).andExpect(status().isOk())
        .andExpect(view().name("body/table/table")).andExpect(model().attribute("table", notNullValue()))
        .andExpect(model().attribute("taxonomies", hasSize(1)));
  }
  
  @Test
  @Transactional
  public void tableController_02_addFavourite() throws Exception {
    mockField(tableEntityController, "tableEntityService", tableEntityService);

    mockMvc.perform(post("/table/favourite").header("Referer", "favourite").param("fqdn", getTestTable().getFqdn()))
        .andExpect(status().isFound()).andExpect(view().name("redirect:favourite"));
  }

  @Test
  @Transactional
  public void tableController_03_setTaxonomy() throws Exception {
    mockField(tableEntityController, "tableEntityService", tableEntityService);

    mockMvc
        .perform(
            post("/table/categoryobjects").header("Referer", "taxonomy").param("fqdn", getTestTable().getFqdn())
                .param("SomeCategoryObjects", "101").param("tags", TEST_TAG)).andExpect(status().isFound())
        .andExpect(view().name("redirect:taxonomy#taxonomyContent"));
  }

  @Test
  @Transactional
  public void tableController_04_getLineage() throws Exception {
    mockField(tableEntityController, "tableEntityService", tableEntityService);

    mockMvc.perform(get("/table/view/lineage").param("fqdn", getTestTable().getFqdn())).andExpect(status().isOk());
  }
  
  @Test
  @Transactional
  public void tableController_05_setOwner() throws Exception {
    mockField(tableEntityController, "tableEntityService", tableEntityService);

    mockMvc.perform(post("/table/owner")
      .param("fqdn", getTestTable().getFqdn())
      .param("person", getTestUser().getFullname())).andExpect(status().isFound());
  }
  
  @Test
  @Transactional
  public void tableController_06_setTableProperties() throws Exception {
    mockField(tableEntityController, "tableEntityService", tableEntityService);

    mockMvc.perform(post("/admin/table")
      .param("fqdn", getTestTable().getFqdn())
      .param("dataTimestampField", "ts")
      .param("dataTimestampFieldFormat", "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''")).andExpect(status().isFound());
  }
  
  @Test
  @Transactional
  public void tableController_07_increaseViewCount() throws Exception {
    mockField(tableEntityController, "tableEntityService", tableEntityService);

    mockMvc.perform(post("/table/viewcount")
      .param("fqdn", getTestTable().getFqdn())).andExpect(status().isOk());
  }
  
  @Test
  @Transactional
  public void tableController_08_getLineageDetail() throws Exception {
    mockField(tableEntityController, "tableEntityService", tableEntityService);

    mockMvc.perform(get("/table/view/lineage/detail")
      .param("fqdn", getTestTable().getFqdn())
      .param("type", "Table")).andExpect(status().isOk());
  }
  
  @Test
  @Transactional
  public void tableController_09_getSample() throws Exception {
    mockField(tableEntityController, "tableEntityService", tableEntityService);

    mockMvc.perform(get("/table/view/sample")
      .param("fqdn", getTestTable().getFqdn())).andExpect(status().isOk());
  }
  
  @Test
  @Transactional
  public void tableController_10_getPartition() throws Exception {
    mockField(tableEntityController, "tableEntityService", tableEntityService);

    mockMvc.perform(get("/table/view/views")
      .param("fqdn", getTestTable().getFqdn())
      .param("partitionPage", "1")).andExpect(status().isOk());
  }
  
  @Test
  @Transactional
  public void tableController_11_getSampleFilter() throws Exception {
    mockField(tableEntityController, "tableEntityService", tableEntityService);

    mockMvc.perform(get("/table/view/samplefilter")
      .param("fqdn", getTestTable().getFqdn())).andExpect(status().isOk());
  }
  
  @Test
  @Transactional
  public void tableController_12_getParameterValues() throws Exception {
    mockField(tableEntityController, "tableEntityService", tableEntityService);

    mockMvc.perform(get("/table/view/parametervalues").param("fqdn", getTestTable().getFqdn()))
      .andExpect(status().isOk());
  }
  
  @Test
  @Transactional
  public void tableController_13_getTransitivDependencies() throws Exception {
    mockField(tableEntityController, "tableEntityService", tableEntityService);

    mockMvc.perform(get("/table").param("fqdn", getTestTable().getFqdn()).param("transitive", "true")).andExpect(status().isOk())
        .andExpect(view().name("body/table/table")).andExpect(model().attribute("table", notNullValue()));
  }
  
}
