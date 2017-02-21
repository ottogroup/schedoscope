package org.schedoscope.metascope.controller;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.service.MetascopeDataDistributionService;
import org.schedoscope.metascope.service.MetascopeTableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

@ActiveProfiles(value = "test")
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment= SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class MetascopeDataDistributionControllerTest {

  @Autowired
  protected TestRestTemplate restTemplate;

  @Autowired
  private MetascopeTableService metascopeTableService;

  @MockBean
  private MetascopeDataDistributionService metascopeDataDistributionService;

  @Before
  public void setup() {
    when(metascopeDataDistributionService.checkStatus(any(MetascopeTable.class))).thenReturn(MetascopeDataDistributionService.Status.NotAvailable);
    Mockito.doNothing().when(metascopeDataDistributionService).calculateDistribution(any(MetascopeTable.class));

    MetascopeTable table = new MetascopeTable();
    table.setFqdn("test");
    metascopeTableService.save(table);
  }

  @Test
  public void sometest() throws Exception {
    HttpHeaders headers = new HttpHeaders();
    headers.set("Referer", "/test");

    HttpEntity<String> entity = new HttpEntity<String>("parameters", headers);

    ResponseEntity<String> response = this.restTemplate.exchange("/datadistribution/start?fqdn=test", HttpMethod.POST, entity, String.class);
    assertEquals(302, response.getStatusCodeValue());
    assertTrue(response.getHeaders().get("Location").get(0).endsWith("/test#datadistributionContent"));
  }

}
