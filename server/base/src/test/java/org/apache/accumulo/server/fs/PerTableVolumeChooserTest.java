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
package org.apache.accumulo.server.fs;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

public class PerTableVolumeChooserTest {
  private static final int REQUIRED_NUMBER_TRIES = 20; // times to call choose for likely exercising of each preferred volume
  private static final String[] ALL_OPTIONS = new String[] {"1", "2", "3"};
  public static final String INVALID_CHOOSER_CLASSNAME = "MysteriousVolumeChooser";
  private ServerConfigurationFactory mockedServerConfigurationFactory;
  private TableConfiguration mockedTableConfiguration;
  private PerTableVolumeChooser perTableVolumeChooser;
  private AccumuloConfiguration mockedAccumuloConfiguration;

  @Before
  public void before() throws Exception {
    perTableVolumeChooser = new PerTableVolumeChooser();

    mockedServerConfigurationFactory = EasyMock.createMock(ServerConfigurationFactory.class);
    Field field = perTableVolumeChooser.getClass().getDeclaredField("serverConfs");
    field.setAccessible(true);
    field.set(perTableVolumeChooser, mockedServerConfigurationFactory);

    mockedTableConfiguration = EasyMock.createMock(TableConfiguration.class);
    mockedAccumuloConfiguration = EasyMock.createMock(AccumuloConfiguration.class);
  }

  private void configureDefaultVolumeChooser(String className) {
    EasyMock.expect(mockedServerConfigurationFactory.getSystemConfiguration()).andReturn(mockedAccumuloConfiguration).anyTimes();
    EasyMock.expect(mockedAccumuloConfiguration.get(PerTableVolumeChooser.TABLE_VOLUME_CHOOSER)).andReturn(className).anyTimes();
  }

  private void configureChooserForTable(String className) {
    EasyMock.expect(mockedServerConfigurationFactory.getTableConfiguration(EasyMock.<Table.ID> anyObject())).andReturn(mockedTableConfiguration).anyTimes();
    EasyMock.expect(mockedTableConfiguration.get(Property.TABLE_CLASSPATH)).andReturn(null).anyTimes();
    EasyMock.expect(mockedTableConfiguration.get(PerTableVolumeChooser.TABLE_VOLUME_CHOOSER)).andReturn(className).anyTimes();
  }

  private void configureDefaultContextVolumeChooser(String className) {
    EasyMock.expect(mockedAccumuloConfiguration.get(PerTableVolumeChooser.DEFAULT_SCOPED_VOLUME_CHOOSER)).andReturn(className).anyTimes();
  }

  private void configureContextVolumeChooser(String className) {
    EasyMock.expect(mockedAccumuloConfiguration.get(PerTableVolumeChooser.SCOPED_VOLUME_CHOOSER("logger"))).andReturn(className).anyTimes();
  }

  private Set<String> chooseRepeatedlyForTable() throws AccumuloException {
    VolumeChooserEnvironment volumeChooserEnvironment = new VolumeChooserEnvironment(Optional.of(new Table.ID("h")));
    Set<String> results = new HashSet<String>();
    for (int i = 0; i < REQUIRED_NUMBER_TRIES; i++) {
      results.add(perTableVolumeChooser.choose(volumeChooserEnvironment, ALL_OPTIONS));
    }
    return results;
  }

  public static class VolumeChooserAlwaysOne extends VolumeChooserForFixedVolume {
    public VolumeChooserAlwaysOne() {
      super("1");
    }
  }

  public static class VolumeChooserAlwaysTwo extends VolumeChooserForFixedVolume {
    public VolumeChooserAlwaysTwo() {
      super("2");
    }
  }

  public static class VolumeChooserAlwaysThree extends VolumeChooserForFixedVolume {
    public VolumeChooserAlwaysThree() {
      super("3");
    }
  }

  public static class VolumeChooserForFixedVolume implements VolumeChooser {
    private final String onlyValidOption;

    public VolumeChooserForFixedVolume(String fixedVolume) {
      onlyValidOption = fixedVolume;
    }

    @Override
    public String choose(VolumeChooserEnvironment env, String[] options) throws AccumuloException {
      for (String option : options) {
        if (onlyValidOption.equals(option)) {
          return onlyValidOption;
        }
      }
      return null;
    }
  }

  private Set<String> chooseRepeatedlyForContext() throws AccumuloException {
    VolumeChooserEnvironment volumeChooserEnvironment = new VolumeChooserEnvironment(Optional.empty());
    volumeChooserEnvironment.setScope("logger");
    Set<String> results = new HashSet<String>();

    for (int i = 0; i < REQUIRED_NUMBER_TRIES; i++) {
      results.add(perTableVolumeChooser.choose(volumeChooserEnvironment, ALL_OPTIONS));
    }
    return results;
  }

  @Test
  public void testEmptyEnvUsesRandomChooser() throws Exception {
    VolumeChooserEnvironment volumeChooserEnvironment = new VolumeChooserEnvironment(Optional.empty());
    Set<String> results = new HashSet<String>();
    for (int i = 0; i < REQUIRED_NUMBER_TRIES; i++) {
      results.add(perTableVolumeChooser.choose(volumeChooserEnvironment, ALL_OPTIONS));
    }

    Assert.assertEquals(Sets.newHashSet(Arrays.asList(ALL_OPTIONS)), results);
  }

  @Test
  public void testTableConfig() throws Exception {
    configureDefaultVolumeChooser(VolumeChooserAlwaysOne.class.getName());
    configureChooserForTable(VolumeChooserAlwaysTwo.class.getName());

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    Set<String> results = chooseRepeatedlyForTable();

    EasyMock.verify(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);
    Assert.assertEquals(Sets.newHashSet(Arrays.asList("2")), results);
  }

  @Test(expected = AccumuloException.class)
  public void testTableMisconfigured() throws Exception {
    configureDefaultVolumeChooser(VolumeChooserAlwaysOne.class.getName());
    configureChooserForTable(INVALID_CHOOSER_CLASSNAME);

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    chooseRepeatedlyForTable();

    EasyMock.verify(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);
  }

  @Test(expected = AccumuloException.class)
  public void testTableMissing() throws Exception {
    configureDefaultVolumeChooser(VolumeChooserAlwaysOne.class.getName());
    configureChooserForTable(null);

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    chooseRepeatedlyForTable();

    EasyMock.verify(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);
  }

  @Test(expected = AccumuloException.class)
  public void testTableMisconfiguredAndDefaultEmpty() throws Exception {
    configureDefaultVolumeChooser("");
    configureChooserForTable(INVALID_CHOOSER_CLASSNAME);

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    chooseRepeatedlyForTable();
  }

  @Test(expected = AccumuloException.class)
  public void testTableEmptyConfig() throws Exception {
    configureDefaultVolumeChooser(VolumeChooserAlwaysThree.class.getName());
    configureChooserForTable("");

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    chooseRepeatedlyForTable();

    EasyMock.verify(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);
  }

  @Test(expected = AccumuloException.class)
  public void testTableAndDefaultEmpty() throws Exception {
    configureDefaultVolumeChooser("");
    configureChooserForTable("");

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    chooseRepeatedlyForTable();
  }

  @Test
  public void testContextConfig() throws Exception {
    configureDefaultVolumeChooser(VolumeChooserAlwaysThree.class.getName());
    configureContextVolumeChooser(VolumeChooserAlwaysOne.class.getName());

    EasyMock.replay(mockedServerConfigurationFactory, mockedAccumuloConfiguration);

    Set<String> results = chooseRepeatedlyForContext();

    EasyMock.verify(mockedServerConfigurationFactory, mockedAccumuloConfiguration);
    Assert.assertEquals(Sets.newHashSet(Arrays.asList("1")), results);
  }

  @Test(expected = AccumuloException.class)
  public void testContextMisconfigured() throws Exception {
    configureDefaultVolumeChooser(VolumeChooserAlwaysThree.class.getName());
    configureContextVolumeChooser(INVALID_CHOOSER_CLASSNAME);

    EasyMock.replay(mockedServerConfigurationFactory, mockedAccumuloConfiguration);

    chooseRepeatedlyForContext();

    EasyMock.verify(mockedServerConfigurationFactory, mockedAccumuloConfiguration);
  }

  @Test(expected = AccumuloException.class)
  public void testContextMissing() throws Exception {
    configureDefaultVolumeChooser(VolumeChooserAlwaysTwo.class.getName());
    configureContextVolumeChooser(null);
    configureDefaultContextVolumeChooser(null);

    EasyMock.replay(mockedServerConfigurationFactory, mockedAccumuloConfiguration);

    Set<String> results = chooseRepeatedlyForContext();

    EasyMock.verify(mockedServerConfigurationFactory, mockedAccumuloConfiguration);
    Assert.assertEquals(Sets.newHashSet(Arrays.asList("2")), results);
  }

  @Test(expected = AccumuloException.class)
  public void testContextMisconfiguredAndDefaultEmpty() throws Exception {
    configureDefaultVolumeChooser("");
    configureChooserForTable("");
    configureContextVolumeChooser(INVALID_CHOOSER_CLASSNAME);

    EasyMock.replay(mockedServerConfigurationFactory, mockedAccumuloConfiguration);

    chooseRepeatedlyForContext();
  }

  @Test(expected = AccumuloException.class)
  public void testContextAndDefaultBothEmpty() throws Exception {
    this.configureDefaultVolumeChooser("");
    configureContextVolumeChooser("");

    EasyMock.replay(mockedServerConfigurationFactory, mockedAccumuloConfiguration);

    chooseRepeatedlyForContext();
  }

  @Test(expected = AccumuloException.class)
  public void testContextEmptyConfig() throws Exception {
    configureDefaultVolumeChooser(VolumeChooserAlwaysTwo.class.getName());
    configureContextVolumeChooser("");

    EasyMock.replay(mockedServerConfigurationFactory, mockedAccumuloConfiguration);

    chooseRepeatedlyForContext();

    EasyMock.verify(mockedServerConfigurationFactory, mockedAccumuloConfiguration);
  }
}
