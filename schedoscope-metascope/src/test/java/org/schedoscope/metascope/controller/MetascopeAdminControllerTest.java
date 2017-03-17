package org.schedoscope.metascope.controller;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@ActiveProfiles(value = "test")
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class MetascopeAdminControllerTest {

    @Autowired
    protected TestRestTemplate restTemplate;

    @Test
    public void sometest() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Referer", "/test");
        HttpEntity<String> entity = new HttpEntity<String>("parameters", headers);

        ResponseEntity<String> response = this.restTemplate.exchange("/admin/sync", HttpMethod.POST, entity, String.class);
        assertEquals(302, response.getStatusCodeValue());
        assertTrue(response.getHeaders().get("Location").get(0).endsWith("/test"));
    }

}
