package org.schedoscope.metascope.repository;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.schedoscope.metascope.model.MetascopeTaxonomy;
import org.schedoscope.metascope.model.MetascopeUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@DataJpaTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class MetascopeUserRepositoryTest {

  @Autowired
  private TestEntityManager entityManager;

  @Autowired
  private MetascopeUserRepository metascopeUserRepository;

  private MetascopeUser metascopeUser;

  @Before
  public void setup() {
    MetascopeUser tmpMetascopeUser = new MetascopeUser();
    tmpMetascopeUser.setUsername("testUser");
    tmpMetascopeUser.setEmail("test@ottogroup.com");
    tmpMetascopeUser.setFullname("Foo Bar");
    this.metascopeUser = this.entityManager.persist(tmpMetascopeUser);
  }

  @Test
  public void findByUserNameTest() {
    MetascopeUser user = metascopeUserRepository.findByUsername("testUser");

    assertNotNull(user);
    assertEquals("testUser", user.getUsername());
    assertEquals("test@ottogroup.com", user.getEmail());
    assertEquals("Foo Bar", user.getFullname());
  }

  @Test
  public void findByEmailTest() {
    MetascopeUser user = metascopeUserRepository.findByEmail("test@ottogroup.com");

    assertNotNull(user);
    assertEquals("testUser", user.getUsername());
    assertEquals("test@ottogroup.com", user.getEmail());
    assertEquals("Foo Bar", user.getFullname());
  }

  @Test
  public void findByFullnameTest() {
    MetascopeUser user = metascopeUserRepository.findByFullname("Foo Bar");

    assertNotNull(user);
    assertEquals("testUser", user.getUsername());
    assertEquals("test@ottogroup.com", user.getEmail());
    assertEquals("Foo Bar", user.getFullname());
  }

}
