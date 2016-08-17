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

package org.schedoscope.export.jdbc.exception;

/**
 * An exception that is thrown if an unrecoverable error occurs.
 */
public class UnrecoverableException extends Exception {

    private static final long serialVersionUID = 6260866612384281711L;

    /**
     * Default ctor.
     */
    public UnrecoverableException() {
        super();
    }

    /**
     * Initializes the exception with a message.
     *
     * @param message
     *            The exception message.
     */
    public UnrecoverableException(String message) {
        super(message);
    }

    /**
     * Initializes the exception from a throwable.
     *
     * @param cause
     *            The exception to initialize from.
     */
    public UnrecoverableException(Throwable cause) {
        super(cause);
    }

    /**
     * Initializes the exception from a throwable and a message.
     *
     * @param message
     *            The exception message.
     * @param cause
     *            The exception to initialize from.
     */
    public UnrecoverableException(String message, Throwable cause) {
        super(message, cause);
    }

}
