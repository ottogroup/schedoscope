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

package org.schedoscope.export.testsupport;

import java.io.IOException;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.ClearTextPasswordEncryptor;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.UserFactory;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.sshd.SshServer;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.common.util.KeyUtils;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.PasswordAuthenticator;
import org.apache.sshd.server.PublickeyAuthenticator;
import org.apache.sshd.server.UserAuth;
import org.apache.sshd.server.auth.UserAuthPassword;
import org.apache.sshd.server.auth.UserAuthPublicKey;
import org.apache.sshd.server.command.ScpCommandFactory;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.sftp.subsystem.SftpSubsystem;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.KeyPair;

public class EmbeddedFtpAftpServer {

	private static final String FTP_SERVER_DIR = "/tmp";

	private FtpServer ftpd;

	private SshServer sshd;

	public void startEmbeddedFtpServer() throws FtpException {

		PropertiesUserManagerFactory propertyFactory = new PropertiesUserManagerFactory();
		propertyFactory.setPasswordEncryptor(new ClearTextPasswordEncryptor());

		UserFactory userFactory = new UserFactory();
		userFactory.setName("user1");
		userFactory.setPassword("pass1");
		userFactory.setHomeDirectory(FTP_SERVER_DIR);

		List<Authority> auths = new ArrayList<Authority>();
		Authority auth = new WritePermission();
		auths.add(auth);
		userFactory.setAuthorities(auths);

		User user = userFactory.createUser();
		UserManager userManager = propertyFactory.createUserManager();
		userManager.save(user);


		ListenerFactory listenerFactory = new ListenerFactory();
		listenerFactory.setPort(2221);

		FtpServerFactory serverFactory = new FtpServerFactory();
		serverFactory.setUserManager(userManager);
		serverFactory.addListener("default", listenerFactory.createListener());

		ftpd = serverFactory.createServer();
		ftpd.start();
	}

	public void stopEmbeddedFtpServer() {

		ftpd.stop();
	}

	public void startEmbeddedSftpServer() throws IOException {

		sshd = SshServer.setUpDefaultServer();
		sshd.setPort(12222);
		sshd.setHost("localhost");

		List<NamedFactory<UserAuth>> userAuthFactories = new ArrayList<NamedFactory<UserAuth>>();
		userAuthFactories.add(new UserAuthPassword.Factory());
		userAuthFactories.add(new UserAuthPublicKey.Factory());
		sshd.setUserAuthFactories(userAuthFactories);

		sshd.setPasswordAuthenticator(new PasswordAuthenticator() {

			@Override
			public boolean authenticate(String username, String password, ServerSession session) {
				return "user1".equals(username) && "pass1".equals(password);
			}
		});

		sshd.setPublickeyAuthenticator(new PublickeyAuthenticator() {

			@Override
			public boolean authenticate(String username, PublicKey key, ServerSession session) {
				if (username.equals("user1")) {

					try {
						Set<String> keys = new HashSet<String>();

						JSch jsch = new JSch();
						String key1= KeyPair.load(jsch,
								"src/test/resources/keys/id_rsa_not_encrypted",
								"src/test/resources/keys/id_rsa_not_encrypted.pub").getFingerPrint();
						String key2= KeyPair.load(jsch,
								"src/test/resources/keys/id_rsa_encrypted",
								"src/test/resources/keys/id_rsa_encrypted.pub").getFingerPrint();

						keys.add(key1);
						keys.add(key2);

						if (keys.contains(KeyUtils.getFingerPrint(key))) {
							return true;
						}
					} catch (JSchException e) {
					}
				}
				return false;
			}
		});

		sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());
		sshd.setSubsystemFactories(Arrays.<NamedFactory<Command>>asList(new SftpSubsystem.Factory()));
		sshd.setCommandFactory(new ScpCommandFactory());
		sshd.setFileSystemFactory(new VirtualFileSystemFactory(FTP_SERVER_DIR));

		sshd.start();
	}

	public void stopEmbeddedSftpServer() throws InterruptedException {

		sshd.stop();
	}
}
