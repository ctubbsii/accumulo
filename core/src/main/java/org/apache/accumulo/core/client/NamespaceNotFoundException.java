/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.client;

import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;

/**
 * Thrown when the namespace specified doesn't exist when it was expected to
 */
public class NamespaceNotFoundException extends Exception {
  /**
   * Exception to throw if an operation is attempted on a namespace that doesn't exist.
   */
  private static final long serialVersionUID = 1L;

  private final String namespace;

  /**
   * @param namespaceId the internal id of the namespace that was sought
   * @param namespaceName the visible name of the namespace that was sought
   * @param description the specific reason why it failed
   */
  public NamespaceNotFoundException(String namespaceId, String namespaceName, String description) {
    super(
        "Namespace" + (namespaceName != null && !namespaceName.isEmpty() ? " " + namespaceName : "")
            + (namespaceId != null && !namespaceId.isEmpty() ? " (Id=" + namespaceId + ")" : "")
            + " does not exist"
            + (description != null && !description.isEmpty() ? " (" + description + ")" : ""));
    this.namespace = namespaceName;
  }

  /**
   * @param namespaceId the internal id of the namespace that was sought
   * @param namespaceName the visible name of the namespace that was sought
   * @param description the specific reason why it failed
   * @param cause the exception that caused this failure
   */
  public NamespaceNotFoundException(String namespaceId, String namespaceName, String description,
      Throwable cause) {
    this(namespaceId, namespaceName, description);
    super.initCause(cause);
  }

  /**
   * @param e constructs an exception from a thrift exception
   */
  public NamespaceNotFoundException(ThriftTableOperationException e) {
    this(e.getTableId(), e.getTableName(), e.getDescription(), e);
  }

  /**
   * @return the name of the namespace sought
   */
  public String getNamespaceName() {
    return namespace;
  }
}
