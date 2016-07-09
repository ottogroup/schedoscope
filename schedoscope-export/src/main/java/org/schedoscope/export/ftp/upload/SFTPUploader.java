package org.schedoscope.export.ftp.upload;

import java.io.File;
import java.io.IOException;
import java.net.URI;

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

public class SFTPUploader {

	private static final Log LOG = LogFactory.getLog(SFTPUploader.class);

	private StandardFileSystemManager fsManager = null;

	private FileSystemOptions opts = null;

	public SFTPUploader(URI uri, String user, String pass, String keyFile, boolean passive, boolean userRoot, Configuration conf) throws IOException {

		this.fsManager = new StandardFileSystemManager();
		this.fsManager.init();
		this.opts = new FileSystemOptions();

		String scheme = uri.getScheme();

		HdfsFileSystemConfigBuilder.getInstance().setConfigConfiguration(opts, conf);

		if (scheme.equals("sftp")) {


			if (keyFile != null) {
				// set up authentication - pub/priv key
				LOG.debug("setting up pub key authentication for sftp protocol");
				IdentityInfo ident = new IdentityInfo(new File(keyFile));
				SftpFileSystemConfigBuilder.getInstance().setIdentityInfo(opts, ident);
			} else {
				// set up authentication - user/pass
				LOG.debug("setting up user/pass authentication for sftp protocol");
				UserAuthenticator auth = new StaticUserAuthenticator(null, user, pass);
				DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
			}

			SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, userRoot);

		} else if(scheme.equals("ftp")) {


			// set up ftp connection and set active/passive mode according to configuration
			LOG.debug("setting up user/pass authentication for ftp protocol");
			FtpFileSystemConfigBuilder.getInstance().setPassiveMode(opts, passive);
			FtpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, false);

		} else {
			throw new IllegalArgumentException("unsupported schemecol: " + scheme);
		}
	}

	public void uploadFile(String inFile, String outFile) throws FileSystemException {

		FileObject local = fsManager.resolveFile(inFile);
		FileObject remote = fsManager.resolveFile(outFile);
		LOG.debug("copy " + local + " to " + remote);
		remote.copyFrom(local, new AllFileSelector());
	}
}
