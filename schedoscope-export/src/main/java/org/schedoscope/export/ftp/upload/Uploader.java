/**
 * Copyright 2016 Otto (GmbH & Co KG)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.schedoscope.export.ftp.upload;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.KeyPair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.hdfs.HdfsFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.sftp.IdentityInfo;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;

/**
 * The class takes care of setting up the (S)FTP connection and provides
 * a function to upload a file.
 */
public class Uploader {

    private static final Log LOG = LogFactory.getLog(Uploader.class);

    private StandardFileSystemManager fsManager = null;

    private FileSystemOptions opts = null;

    /**
     * The constructor to initialize a user name / password (s)ftp connection.
     *
     * @param user       The username to use.
     * @param pass       The password to use.
     * @param conf       The Hadoop Configuration object, needed to initialize HDFS.
     * @param passive    A flag to use FTP passive mode (only for ftp connections).
     * @param userIsRoot A flag indicating the user dir is (s)ftp root dir.
     * @throws IOException Is thrown if an error occures.
     */
    public Uploader(String user, String pass, Configuration conf, boolean passive, boolean userIsRoot)
            throws IOException {

        initFileSystem(conf, passive, userIsRoot);

        // set up authentication - user/pass for ftp and sftp
        LOG.debug("setting up user/pass authentication");

        UserAuthenticator auth = new StaticUserAuthenticator(null, user, pass);
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
    }

    /**
     * A constructor to initialize a user name / pub key sftp connection.
     *
     * @param user       The username to use.
     * @param keyFile    The private key file.
     * @param passphrase The passphrase to use (can be null)
     * @param conf       The Hadoop Configuration object.
     * @param passive    A flag to use FTP passive mode (only for ftp connections).
     * @param userIsRoot A flag indicating the user dir is (s)ftp root dir.
     * @throws IOException Is thrown if an error occurs.
     */
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

    /**
     * A method to check if a private key is passphrase protected.
     *
     * @param keyFile The private key file to check.
     * @throws Exception Is thrown if an error occurs.
     */
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

    /**
     * A method to copy a file from src (hdfs) to (s)ftp (remote).
     *
     * @param inFile  The input file to copy.
     * @param outFile The output file to create.
     * @throws FileSystemException Is thrown if an error occurs.
     */
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
