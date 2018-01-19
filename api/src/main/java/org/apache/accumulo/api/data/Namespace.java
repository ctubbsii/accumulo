/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.api.data;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.api.client.BadConfiguration;
import org.apache.accumulo.api.client.NamespaceNotEmpty;
import org.apache.accumulo.api.client.NamespaceNotFound;
import org.apache.accumulo.api.client.TableExists;
import org.apache.accumulo.api.client.TableNotFound;
import org.apache.accumulo.api.plugins.TableConstraint;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public interface Namespace {

  /**
   * A builder for constructing an Accumulo {@link Table}.
   */
  public interface Builder {

    /**
     * Build the {@link Table} object from the options previously provided.
     *
     * @return an instance of {@link Table}, configured from the current state of this builder
     * @throws BadConfiguration
     *           if the provided configuration is malformed or insufficient to build a table
     */
    Namespace build() throws BadConfiguration, TableExists;

    /**
     * Additional properties to set on the table's configuration after all other options have been serialized to the table's configuration.
     *
     * @param properties
     *          a map of keys/values representing additional options to set
     * @return this, with the additional properties set
     */
    Namespace.Builder withAdditionalProperties(Map<String,String> properties);

    /**
     * Apply a constraint to a table's properties.
     *
     * @param constraint
     *          the constraint to be configured for the table
     * @return this, with the constraint configured
     */
    Namespace.Builder withConstraint(TableConstraint constraint);

    /**
     * Apply an iterator.
     *
     * @param iterator
     *          the iterator settings to configure for all the tables in this namespace, at the next available priority
     * @return this, with the iterator configured
     */
    Namespace.Builder withIterator(IteratorSetting iterator);

    /**
     * Apply an iterator at a specified priority.
     *
     * @param iterator
     *          the iterator settings to configure for all tables in this namespace, at the specified priority
     * @param priority
     *          the positive priority; lower numbered iterators iterate over data before higher numbered ones
     * @return this, with the iterator configured
     */
    Namespace.Builder withIterator(IteratorSetting iterator, int priority);

  }

  // @XmlJavaTypeAdapter(JaxbAbstractIdSerializer.class)
  public class ID extends AbstractId {
    private static final long serialVersionUID = 1L;
    static final Cache<String,ID> cache = CacheBuilder.newBuilder().weakValues().build();

    public static final ID ACCUMULO = of("+accumulo");
    public static final ID DEFAULT = of("+default");

    private ID(String canonical) {
      super(canonical);
    }

    /**
     * Get a Namespace.ID object for the provided canonical string.
     *
     * @param canonical
     *          Namespace ID string
     * @return Namespace.ID object
     */
    public static Namespace.ID of(final String canonical) {
      try {
        return cache.get(canonical, () -> new Namespace.ID(canonical));
      } catch (ExecutionException e) {
        throw new AssertionError("This should never happen: ID constructor should never return null.");
      }
    }
  }

  public static final String ACCUMULO = "accumulo";
  public static final String DEFAULT = "";
  public static final String SEPARATOR = ".";

  /**
   * Delete this namespace, if it exists and is empty.
   *
   * @throws NamespaceNotFound
   *           if the namespace doesn't exist
   * @throws UnsupportedOperationException
   *           if an attempt is made to delete a built-in namespace
   */
  void delete() throws NamespaceNotFound, NamespaceNotEmpty, UnsupportedOperationException;

  /**
   * Check if this namespace still exists (it may have been deleted by another process/thread).
   *
   * @return true if it exists and false otherwise
   */
  boolean exists();

  ID getId();

  /**
   * Get the name of this namespace, as it is currently known. This is either the name used to instantiate this object or the name resulting from the last
   * successful execution of {@link #rename(String)} by this object. This may not reflect the true name of the namespace, because another thread could have
   * renamed it.
   *
   * <p>
   * This method is for informational purposes only, and is not used to execute operations. Regardless of the currently known name, any operations performed on
   * this namespace will be executed with the namespace {@link ID}, which never changes and is resolved immediately when this object is constructed.
   *
   * @return the currently known name of this namespace
   */
  String getName();

  boolean isDefaultNamespace();

  boolean isSystemNamespace();

  boolean isUserNamespace();

  /**
   * Construct a new table, with the desired options.
   *
   * @return a table builder
   */
  Table.Builder tableBuilder(String name);

  /**
   * Remove a property from this namespace's configuration
   *
   * @param key
   *          the property to remove
   */
  void removeProperty(final String key);

  /**
   * Rename the current namespace to the given name
   *
   * @param newName
   *          the target name
   * @throws UnsupportedOperationException
   *           if an attempt is made to rename a built-in namespace
   */
  void rename(final String newName) throws UnsupportedOperationException;

  /**
   * Sets a property with the given value in this namespace's configuration
   *
   * @param key
   *          the property to set
   * @param value
   *          the value of the property being set
   */
  void setProperty(final String key, final String value);

  /**
   * Retrieve a specific table, by its current name.
   *
   * @param name
   *          the specified name can be the fully-qualified table name, which includes the namespace, or unqualified. If it is fully-qualified, it must match
   *          this namespace's name.
   * @return the table requested
   * @throws TableNotFound
   *           if the table cannot be found with the specified name
   */
  Table table(String name) throws TableNotFound;

  /**
   * Retrieve all tables currently in this namespace
   *
   * @return the set of tables in this namespace
   */
  Set<Table> tables();

}
