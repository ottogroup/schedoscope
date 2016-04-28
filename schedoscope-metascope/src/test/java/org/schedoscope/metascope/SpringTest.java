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
package org.schedoscope.metascope;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.schedoscope.metascope.conf.TestSpringConfiguration;
import org.schedoscope.metascope.controller.TableEntityController;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.model.FieldEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.UserEntity;
import org.schedoscope.metascope.model.ViewEntity;
import org.schedoscope.metascope.repository.ActivityEntityRepository;
import org.schedoscope.metascope.repository.BusinessObjectEntityRepository;
import org.schedoscope.metascope.repository.CategoryEntityRepository;
import org.schedoscope.metascope.repository.FieldEntityRepository;
import org.schedoscope.metascope.repository.MetadataEntityRepository;
import org.schedoscope.metascope.repository.ParameterValueEntityRepository;
import org.schedoscope.metascope.repository.SuccessorEntityRepository;
import org.schedoscope.metascope.repository.TableDependencyEntityRepository;
import org.schedoscope.metascope.repository.TableEntityRepository;
import org.schedoscope.metascope.repository.TransformationEntityRepository;
import org.schedoscope.metascope.repository.UserEntityRepository;
import org.schedoscope.metascope.repository.ViewDependencyEntityRepository;
import org.schedoscope.metascope.repository.ViewEntityRepository;
import org.schedoscope.metascope.service.ActivityEntityService;
import org.schedoscope.metascope.service.DocumentationService;
import org.schedoscope.metascope.service.FieldEntityService;
import org.schedoscope.metascope.service.MetadataEntityService;
import org.schedoscope.metascope.service.TableEntityService;
import org.schedoscope.metascope.service.TaxonomyService;
import org.schedoscope.metascope.service.URLService;
import org.schedoscope.metascope.service.UserEntityService;
import org.schedoscope.metascope.tasks.repository.RepositoryDAO;
import org.schedoscope.metascope.util.HiveQueryResult;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationContextLoader;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.context.WebApplicationContext;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterators;

@ActiveProfiles("test")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestSpringConfiguration.class, loader = SpringApplicationContextLoader.class)
@WebAppConfiguration
public class SpringTest {

  protected static final String NON_EXIST_TABLE = "org.schedoscope.metascope.NotExist";
  protected static final String NON_EXIST_USER = "notexist";
  protected static final String NON_EXIST_USER_FULLNAME = "Not Exists";
  protected static final String TEST_ADMIN_USER = "admin";
  protected static final String LOGGED_IN_USER = "anonymous";
  protected static final String TEST_CATEGORY = "TestCategory";
  protected static final String TEST_BUSINESS_OBJECT = "TestBusinessObject";
  protected static final String TEST_TAG = "TestTag";
  protected static final String TO_BE_DELETED = "TO_BE_DELETED";

  private static final String NON_EXIST_PARAMETER = "day";
  private static final String TEST_TABLE_PROCESSED_NODES = "demo_schedoscope_example_osm_processed.nodes";
  private static final String TEST_VIEW_PROCESSED_NODES_201501 = "schedoscope.example.osm.processed/Nodes/2015/01/201501";

  @Autowired
  protected WebApplicationContext wac;

  @Autowired
  protected TableEntityController tableEntityController;

  @Autowired
  protected TableEntityService tableEntityService;
  @Autowired
  protected ActivityEntityService activityEntityService;
  @Autowired
  protected DocumentationService documentationService;
  @Autowired
  protected FieldEntityService fieldEntityService;
  @Autowired
  protected MetadataEntityService metadataEntityService;
  @Autowired
  protected TaxonomyService taxonomyService;
  @Autowired
  protected UserEntityService userEntityService;
  @Autowired
  protected URLService urlService;
  @Autowired
  protected SolrFacade solr;

  @Autowired
  protected TableEntityRepository tableEntityRepository;
  @Autowired
  protected ViewEntityRepository viewEntityRepository;
  @Autowired
  protected UserEntityRepository userEntityRepository;
  @Autowired
  protected FieldEntityRepository fieldEntityRepository;
  @Autowired
  protected TableDependencyEntityRepository tableDependencyEntityRepository;
  @Autowired
  protected TransformationEntityRepository transformationEntityRepository;
  @Autowired
  protected ParameterValueEntityRepository parameterValueEntityRepository;
  @Autowired
  protected ViewDependencyEntityRepository viewDependencyEntityRepository;
  @Autowired
  protected SuccessorEntityRepository successorEntityRepository;
  @Autowired
  protected ActivityEntityRepository activityEntityRepository;
  @Autowired
  protected CategoryEntityRepository categoryEntityRepository;
  @Autowired
  protected BusinessObjectEntityRepository boEntityRepository;
  @Autowired
  protected MetadataEntityRepository metadataEntityRepository;

  @Autowired
  protected RepositoryDAO repo;
  @Autowired
  protected DataSource dataSource;
  
  @Autowired 
  protected MockHttpServletRequest request;

  @Before
  public void setup() {
    /*
     * mock UserEntityService getUser() method, which in production returns the
     * logged in user
     */
    UserEntityService userEntityServiceMock = mock(UserEntityService.class);
    when(userEntityServiceMock.getUser()).thenReturn(getLoggedInUser());
    when(userEntityServiceMock.getAllUser()).thenReturn(new ArrayList<UserEntity>());
    when(userEntityServiceMock.isAdmin()).thenReturn(true);
    when(userEntityServiceMock.isFavourite(any(TableEntity.class))).thenReturn(true);
    LoadingCache<String, HiveQueryResult> sampleCacheMock = getSampleCacheMock();
    when(sampleCacheMock.getUnchecked(any(String.class))).thenReturn(new HiveQueryResult("test"));
    mockField(tableEntityService, "userEntityService", userEntityServiceMock);
    mockField(tableEntityService, "sampleCache", sampleCacheMock);
    mockField(taxonomyService, "userEntityService", userEntityServiceMock);
    mockField(tableEntityController, "userEntityService", userEntityServiceMock);

    /* mock the calls to solr (Solr is tested seperatly) */
    SolrFacade solrFacadeMock = mock(SolrFacade.class);
    mockField(tableEntityService, "solr", solrFacadeMock);
    mockField(documentationService, "solr", solrFacadeMock);
    mockField(taxonomyService, "solr", solrFacadeMock);
  }

  @SuppressWarnings("unchecked")
  private LoadingCache<String, HiveQueryResult> getSampleCacheMock() {
    return mock(LoadingCache.class);
  }

  protected void mockField(Class<?> clazz, String name, Object mock) {
    ReflectionTestUtils.setField(getSpringBean(clazz), name, mock);
  }

  protected void mockField(Object target, String name, Object mock) {
    ReflectionTestUtils.setField(getSpringBean(target.getClass()), name, mock);
  }

  protected Object getSpringBean(Class<?> clazz) {
    return unwrap(getBean(clazz));
  }

  private Object getBean(Class<?> clazz) {
    StaticApplicationContext sac = new StaticApplicationContext(wac);
    Object bean = sac.getBean(clazz);
    sac.close();
    return bean;
  }

  private Object unwrap(Object bean) {
    if (AopUtils.isAopProxy(bean) && bean instanceof Advised) {
      Advised advised = (Advised) bean;
      try {
        bean = advised.getTargetSource().getTarget();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return bean;
  }

  protected int size(Iterable<?> iterable) {
    return Iterators.size(iterable.iterator());
  }

  protected TableEntity getTestTable() {
    return tableEntityRepository.findByFqdn(TEST_TABLE_PROCESSED_NODES);
  }

  protected ViewEntity getTestView() {
    return viewEntityRepository.findByUrlPath(TEST_VIEW_PROCESSED_NODES_201501);
  }

  protected UserEntity getLoggedInUser() {
    return userEntityRepository.findByUsername(LOGGED_IN_USER);
  }

  protected UserEntity getTestUser() {
    return userEntityRepository.findByUsername(TEST_ADMIN_USER);
  }

  protected FieldEntity getNonExistParameter() {
    FieldEntity nonExistingParameter = new FieldEntity();
    nonExistingParameter.setName(NON_EXIST_PARAMETER);
    return nonExistingParameter;
  }

}