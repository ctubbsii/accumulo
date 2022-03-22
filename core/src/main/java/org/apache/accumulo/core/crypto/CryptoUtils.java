/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.crypto;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.FileDecrypter;

public class CryptoUtils {

  /**
   * Read the decryption parameters from the DataInput
   */
  public static byte[] readParams(DataInput in) throws IOException {
    Objects.requireNonNull(in);
    int len = in.readInt();
    byte[] decryptionParams = new byte[len];
    in.readFully(decryptionParams);
    return decryptionParams;
  }

  /**
   * Read the decryption parameters from the DataInput and get the FileDecrypter associated with the
   * provided CryptoService and CryptoEnvironment.Scope.
   */
  public static FileDecrypter getFileDecrypter(CryptoService cs, CryptoEnvironment.Scope scope,
      DataInput in) throws IOException {
    byte[] decryptionParams = readParams(in);
    CryptoEnvironment decEnv = new CryptoEnvironmentImpl(scope, decryptionParams);
    return cs.getFileDecrypter(decEnv);
  }

  /**
   * Write the decryption parameters to the DataOutput
   */
  public static void writeParams(byte[] decryptionParams, DataOutput out) throws IOException {
    Objects.requireNonNull(decryptionParams);
    Objects.requireNonNull(out);
    out.writeInt(decryptionParams.length);
    out.write(decryptionParams);
  }

}
