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
package org.apache.accumulo.tserver;

import static java.util.Collections.singletonList;

import java.nio.ByteBuffer;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.constraints.Constraint;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.security.AuthorizationContainer;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.constraints.SystemEnvironment;

public class TservConstraintEnv implements SystemEnvironment, Constraint.Environment {

  private final ServerContext context;
  private final TCredentials credentials;
  private KeyExtent ke;

  TservConstraintEnv(ServerContext context, TCredentials credentials) {
    this.context = context;
    this.credentials = credentials;
  }

  public void setExtent(KeyExtent ke) {
    this.ke = ke;
  }

  @Override
  public TabletId getTablet() {
    return new TabletIdImpl(ke);
  }

  @Override
  public String getUser() {
    return credentials.getPrincipal();
  }

  @Override
  public AuthorizationContainer getAuthorizationsContainer() {
    return auth -> context.getSecurityOperation().authenticatedUserHasAuthorizations(credentials,
        singletonList(ByteBuffer.wrap(auth.getBackingArray(), auth.offset(), auth.length())));
  }

  @Override
  public ServerContext getServerContext() {
    return context;
  }
}
