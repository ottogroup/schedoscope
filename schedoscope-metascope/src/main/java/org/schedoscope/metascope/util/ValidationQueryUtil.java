/**
 * Copyright 2015 Otto (GmbH & Co KG)
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
package org.schedoscope.metascope.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ValidationQueryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ValidationQueryUtil.class);

    public static String getValidationQuery(String driver) {
        Properties properties = loadProperties();
        return properties.getProperty(driver, "select 1");
    }

    private static Properties loadProperties() {
        String propertyFilename = "db.validation.properties";
        try {
            Properties props = new Properties();
            InputStream resourceAsStream = ValidationQueryUtil.class.getClassLoader().getResourceAsStream(propertyFilename);
            props.load(resourceAsStream);
            resourceAsStream.close();
            return props;
        } catch (IOException e) {
            LOG.error("Could not load db.validation.properties file", e);
            return null;
        }
    }

}