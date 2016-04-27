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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.naming.NamingException;
import javax.naming.directory.Attributes;

import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.UserEntity;
import org.schedoscope.metascope.model.UserEntity.Role;
import org.schedoscope.metascope.repository.UserEntityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ldap.core.DirContextAdapter;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.security.authentication.encoding.ShaPasswordEncoder;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.session.SessionInformation;
import org.springframework.security.core.session.SessionRegistry;
import org.springframework.security.ldap.userdetails.LdapUserDetailsImpl;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Domain-specific methods for users.
 */
@Service
public class UserEntityService {

  private static final Logger LOG = LoggerFactory.getLogger(UserEntityService.class);

  @Autowired
  private UserEntityRepository userEntityRepository;
  @Autowired
  private LdapTemplate ldap;
  @Autowired
  private MetascopeConfig config;

  /**
   * Checks if the user browsing the application is logged in
   * 
   * @return true if the user is logged in, false otherwise
   */
  public boolean isAuthenticated() {
    Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    if (authentication != null) {
      Object principal = authentication.getPrincipal();
      return (principal instanceof String) == false;
    }
    return false;
  }

  /**
   * Get the user object for the logged in user
   * 
   * @return
   * @throws NamingException
   */
  public UserEntity getUser() {
    Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

    if (authentication == null) {
      return null;
    }

    Object principal = authentication.getPrincipal();

    if (principal instanceof LdapUserDetailsImpl) {
      LdapUserDetailsImpl ldapUser = (LdapUserDetailsImpl) principal;
      UserEntity userEntity = userEntityRepository.findByUsername(ldapUser.getUsername());
      if (userEntity == null) {
        createUser(ldapUser.getUsername(), "", "", sha256("" + System.currentTimeMillis()), false, null);
      }

      // sync user with ldap
      userEntity = userEntityRepository.findByUsername(ldapUser.getUsername());
      DirContextAdapter dca = (DirContextAdapter) ldap.lookup(ldapUser.getDn());
      Attributes attr = dca.getAttributes();
      String mail = "";
      String fullname = "";
      try {
        mail = (String) attr.get("mail").get();
        fullname = (String) attr.get("displayName").get();
      } catch (NamingException e) {
        // if not found, ignore ..
      }
      boolean admin = false;
      for (GrantedAuthority authoritiy : ldapUser.getAuthorities()) {
        for (String adminGroup : config.getAdminGroups().split(",")) {
          String role = "ROLE_" + adminGroup.toUpperCase();
          if (authoritiy.getAuthority().equalsIgnoreCase(role)) {
            admin = true;
          }
        }
      }

      boolean changes = false;
      if (userEntity.getEmail() == null || !userEntity.getEmail().equals(mail)) {
        userEntity.setEmail(mail);
        changes = true;
      }
      if (userEntity.getFullname() == null || !userEntity.getFullname().equals(fullname)) {
        userEntity.setFullname(fullname);
        changes = true;
      }

      if (admin) {
        if (!userEntity.isAdmin()) {
          changes = true;
        }
        userEntity.setUserrole(Role.ROLE_ADMIN);
      } else {
        if (userEntity.isAdmin()) {
          changes = true;
        }
        userEntity.setUserrole(Role.ROLE_USER);
      }

      if (changes) {
        userEntityRepository.save(userEntity);
      }
      return userEntity;
    } else if (principal instanceof org.springframework.security.core.userdetails.User) {
      org.springframework.security.core.userdetails.User userDetails = (org.springframework.security.core.userdetails.User) principal;
      UserEntity user = userEntityRepository.findByUsername(userDetails.getUsername());

      if (user == null) {
        LOG.warn("User from session not found. username={}", userDetails.getUsername());
        return null;
      }

      return user;
    }

    return null;
  }

  public Iterable<UserEntity> getAllUser() {
    return userEntityRepository.findAll();
  }

  public List<String> getAllFullNames() {
    List<String> names = new ArrayList<String>();
    for (UserEntity user : getAllUser()) {
      names.add(user.getFullname());
    }
    return names;
  }

  public UserEntity findByUsername(String username) {
    return userEntityRepository.findByUsername(username);
  }

  public UserEntity findByFullname(String fullname) {
    return userEntityRepository.findByFullname(fullname);
  }

  /**
   * Checks if a user exists
   * 
   * @param username
   *          username which is checked if it already exists
   * @return true if user already exist, false otherwise
   */
  public boolean userExists(String username) {
    return username != null && userEntityRepository.findByUsername(username) != null;
  }

  /**
   * Checks if an email exists
   * 
   * @param email
   *          email which is checked if it already exists
   * @return true if email already exist, false otherwise
   */
  public boolean emailExists(String email) {
    return email != null && userEntityRepository.findByEmail(email) != null;
  }

  /**
   * Registers a user in the database
   * 
   * @param username
   *          username of the new user
   * @param email
   *          email of the new user
   * @param fullname
   *          the full name of the user
   * @param password
   *          password of the new user
   * @param admin
   *          has admin rights
   * @param group
   *          the associcated group of the user
   * @return The new user object
   */
  @Transactional
  public void createUser(String username, String email, String fullname, String password, boolean admin, String group) {
    if (!isAuthenticated()) {
      return;
    }

    UserEntity user = new UserEntity();
    user.setUsername(username);
    user.setEmail(email);
    user.setFullname(fullname);
    user.setPasswordHash(sha256(password));
    user.setUsergroup(group);
    user.setFavourites(Collections.<String> emptyList());
    if (admin) {
      user.setUserrole(Role.ROLE_ADMIN);
    } else {
      user.setUserrole(Role.ROLE_USER);
    }
    userEntityRepository.save(user);
    LOG.info("User '{}' has created a new user '{}'", getUser().getUsername(), username);
  }

  @Transactional
  public void editUser(String username, String email, String fullname, String password, boolean admin, String group) {
    if (!isAuthenticated()) {
      return;
    }

    UserEntity user = userEntityRepository.findByUsername(username);

    if (!user.getEmail().equals(email)) {
      return;
    }

    user.setFullname(fullname);

    if (!password.isEmpty()) {
      user.setPasswordHash(sha256(password));
    }

    user.setUsergroup(group);
    if (admin) {
      user.setUserrole(Role.ROLE_ADMIN);
    } else {
      user.setUserrole(Role.ROLE_USER);
    }
    userEntityRepository.save(user);
    LOG.info("User '{}' has edited the user '{}'", getUser().getUsername(), username);
  }

  @Transactional
  public void deleteUser(String username) {
    if (!isAuthenticated()) {
      return;
    }

    UserEntity user = userEntityRepository.findByUsername(username);

    if (user == null) {
      return;
    }

    userEntityRepository.delete(user);
    LOG.info("User '{}' has deleted the user '{}'", getUser().getUsername(), username);
  }

  public List<String> getFavourites() {
    return getUser().getFavourites();
  }

  public boolean isAdmin() {
    if (getUser().isAdmin()) {
      return true;
    }

    Object principal = SecurityContextHolder.getContext().getAuthentication().getPrincipal();

    if (principal instanceof LdapUserDetailsImpl) {
      LdapUserDetailsImpl ldapUser = (LdapUserDetailsImpl) principal;
      for (GrantedAuthority authoritiy : ldapUser.getAuthorities()) {
        for (String adminGroup : config.getAdminGroups().split(",")) {
          String role = "ROLE_" + adminGroup.toUpperCase();
          if (authoritiy.getAuthority().equalsIgnoreCase(role)) {
            return true;
          }
        }
      }
    }

    return false;
  }

  public void createAdminAccount() {
    UserEntity adminUser = userEntityRepository.findByUsername("admin");
    if (adminUser == null) {
      adminUser = new UserEntity();
      adminUser.setUsername("admin");
      adminUser.setPasswordHash("8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918"); // sha256("admin")
      adminUser.setFullname("Admin");
      adminUser.setUserrole("ROLE_ADMIN");
      adminUser.setUsergroup("BUSINESS_USER");
      adminUser.setEmail("admin@metascope.com");
      userEntityRepository.save(adminUser);
    }
  }

  private String sha256(String password) {
    return new ShaPasswordEncoder(256).encodePassword(password, null);
  }

  public void logoutUser(SessionRegistry sessionRegistry, String username) {
    final List<Object> allPrincipals = sessionRegistry.getAllPrincipals();
    for (final Object principal : allPrincipals) {
      if (principal instanceof UserEntity) {
        UserEntity springUserDetails = (UserEntity) principal;
        if (springUserDetails.getUsername().equals(username)) {
          for (SessionInformation sessionInformation : sessionRegistry.getAllSessions(principal, true)) {
            sessionInformation.expireNow();
          }
        }
      }
    }
  }

  public void setUserEntityRepository(UserEntityRepository userEntityRepository) {
    this.userEntityRepository = userEntityRepository;
  }

  public boolean isFavourite(TableEntity tableEntity) {
    List<String> favourites = getFavourites();
    for (String fav : favourites) {
      if (tableEntity.getFqdn().equals(fav)) {
        return true;
      }
    }
    return false;
  }

}