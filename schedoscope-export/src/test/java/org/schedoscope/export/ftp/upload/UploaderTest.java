package org.schedoscope.export.ftp.upload;

import org.junit.Test;

public class UploaderTest {

	@Test
	public void testEncrypetedKey() throws Exception{
		Uploader.checkPrivateKey("src/test/resources/keys/id_rsa_encrypted");
		assert(true);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNotEncryptedKey() throws Exception {
		Uploader.checkPrivateKey("src/test/resources/keys/id_rsa_not_encrypted");
	}
}
