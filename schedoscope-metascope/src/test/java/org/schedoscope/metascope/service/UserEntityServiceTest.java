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
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.schedoscope.metascope.SpringTest;
import org.schedoscope.metascope.model.UserEntity;
import org.schedoscope.metascope.repository.UserEntityRepository;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UserEntityServiceTest extends SpringTest {

	private static final String TEST_ADMIN_USER_FULLNAME = "Admin Admin";
	private static final String LOGGED_IN_USER_FULLNAME = "Foo Bar";

	private UserEntityService userEntityServiceMock;

	@Before
	public void setupLocal() {
		userEntityServiceMock = mock(UserEntityService.class);
		when(userEntityServiceMock.isAuthenticated()).thenReturn(true);
		when(userEntityServiceMock.getUser()).thenReturn(new UserEntity());
		doCallRealMethod().when(userEntityServiceMock).createUser(
				any(String.class), any(String.class), any(String.class),
				any(String.class), any(Boolean.class), any(String.class));
		doCallRealMethod().when(userEntityServiceMock).editUser(
				any(String.class), any(String.class), any(String.class),
				any(String.class), any(Boolean.class), any(String.class));
		doCallRealMethod().when(userEntityServiceMock).deleteUser(
				any(String.class));
		doCallRealMethod().when(userEntityServiceMock).setUserEntityRepository(
				any(UserEntityRepository.class));
		userEntityServiceMock.setUserEntityRepository(userEntityRepository);
	}

	@Test
	public void userService_01_createAdminUser() {
		int userSize = size(userEntityRepository.findAll());

		assertEquals(userSize, 0);

		userEntityServiceMock.createUser(TEST_ADMIN_USER,
				"admin@ottogroup.com", TEST_ADMIN_USER_FULLNAME, "admin", true,
				"OPS_USER");

		Iterable<UserEntity> users = userEntityRepository.findAll();

		assertEquals(size(users), 1);

		UserEntity userEntity = users.iterator().next();

		assertEquals(userEntity.getFullname(), TEST_ADMIN_USER_FULLNAME);
	}

	@Test
	public void userService_02_createUser() {
		int userSize = size(userEntityRepository.findAll());

		assertEquals(userSize, 1);

		userEntityServiceMock.createUser(LOGGED_IN_USER, "test@ottogroup.com",
				LOGGED_IN_USER_FULLNAME, "test", false, "OPS_USER");

		Iterable<UserEntity> users = userEntityRepository.findAll();

		assertEquals(size(users), 2);

		Iterator<UserEntity> iterator = users.iterator();
		iterator.next();
		UserEntity userEntity = iterator.next();

		assertEquals(userEntity.getFullname(), LOGGED_IN_USER_FULLNAME);
	}

	@Test
	public void userService_03_createUserToBeDeleted() {
		int userSize = size(userEntityRepository.findAll());

		assertEquals(userSize, 2);

		userEntityServiceMock.createUser("TO_BE_DELETED",
				"toDelete@ottogroup.com", "To Be Deleted", "test", false,
				"OPS_USER");

		userSize = size(userEntityRepository.findAll());

		assertEquals(userSize, 3);
	}

	@Test
	public void userService_04_editUser() {
		UserEntity userEntity = userEntityRepository
				.findByUsername("TO_BE_DELETED");

		assertEquals(userEntity.getFullname(), "To Be Deleted");

		userEntityServiceMock.editUser("TO_BE_DELETED",
				"toDelete@ottogroup.com", "Edit Name", "test", false,
				"OPS_USER");

		userEntity = userEntityRepository.findByUsername("TO_BE_DELETED");

		assertEquals(userEntity.getFullname(), "Edit Name");
	}

	@Test
	public void userService_05_deleteUser() {
		int userSize = size(userEntityRepository.findAll());

		assertEquals(userSize, 3);

		userEntityServiceMock.deleteUser("TO_BE_DELETED");

		Iterable<UserEntity> allUser = userEntityRepository.findAll();
		userSize = size(allUser);

		assertEquals(userSize, 2);

		for (UserEntity userEntity : allUser) {
			assertFalse(userEntity.getUsername().equals("TO_BE_DELETED"));
		}
	}

}
