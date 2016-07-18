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

package org.schedoscope.export.ftp.upload;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticator;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.hdfs.HdfsFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.sftp.IdentityInfo;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.apache.hadoop.conf.Configuration;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.KeyPair;

public class Uploader {

	private static final Log LOG = LogFactory.getLog(Uploader.class);

	private StandardFileSystemManager fsManager = null;

	private FileSystemOptions opts = null;

	public Uploader(String user, String pass, Configuration conf, boolean passive, boolean userIsRoot)
			throws IOException {

		initFileSystem(conf, passive, userIsRoot);

		// set up authentication - user/pass for ftp and sftp
		LOG.debug("setting up user/pass authentication");

		UserAuthenticator auth = new StaticUserAuthenticator(null, user, pass);
		DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
	}

	public Uploader(String user, File keyFile, String passphrase, Configuration conf, boolean passive,
			boolean userIsRoot) throws IOException {

		initFileSystem(conf, passive, userIsRoot);

		// set up authentication - pub/priv key
		LOG.debug("setting up pub key authentication for sftp protocol");
		UserAuthenticator auth = new StaticUserAuthenticator(null, user, null);
		DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);

		IdentityInfo ident = new IdentityInfo(keyFile);
		SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(opts, "no");
		SftpFileSystemConfigBuilder.getInstance().setUserInfo(opts, new PassphraseUserInfo(passphrase));
		SftpFileSystemConfigBuilder.getInstance().setIdentityInfo(opts, ident);
	}

	public static void checkPrivateKey(String keyFile) throws Exception {

		JSch jsch = new JSch();
		KeyPair keyPair = KeyPair.load(jsch, keyFile);

		if (!keyPair.isEncrypted()) {
			throw new IllegalArgumentException("private key is not protected by a passphrase - aborting");
		}
	}

	private void initFileSystem(Configuration conf, boolean passive, boolean userIsRoot) throws IOException {

		this.fsManager = new StandardFileSystemManager();
		this.fsManager.init();
		this.opts = new FileSystemOptions();

		// configure hdfs file system
		HdfsFileSystemConfigBuilder.getInstance().setConfigConfiguration(opts, conf);

		// configure sftp file system
		SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, userIsRoot);

		// configure ftp file system
		FtpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, userIsRoot);
		FtpFileSystemConfigBuilder.getInstance().setPassiveMode(opts, passive);
	}

	public void uploadFile(String inFile, String outFile) throws FileSystemException {

		FileObject local = fsManager.resolveFile(inFile);
		FileObject remote = fsManager.resolveFile(outFile, opts);
		LOG.debug("copy " + local + " to " + remote);
		remote.copyFrom(local, new AllFileSelector());
	}

	public void closeFilesystem() {

		fsManager.close();
	}
}
