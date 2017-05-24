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
package org.apache.accumulo.test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.fs.PerTableVolumeChooser;
import org.apache.accumulo.server.fs.PreferredVolumeChooser;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;

public class VolumeChooserForNonTableScopeIT extends ConfigurableMacBase {

  private File volDirBase;
  private Path v1, v2, v4;
  private String systemPreferredVolumes;

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Get 2 tablet servers
    cfg.setNumTservers(2);

    // Set the general volume chooser to the PerTableVolumeChooser so that different choosers can be specified
    Map<String,String> siteConfig = new HashMap<String,String>();
    siteConfig.put(Property.GENERAL_VOLUME_CHOOSER.getKey(), PerTableVolumeChooser.class.getName());

    // a default is required
    siteConfig.put(Property.TABLE_VOLUME_CHOOSER.getKey(), PreferredVolumeChooser.class.getName());

    // Set up 4 different volume paths
    File baseDir = cfg.getDir();
    volDirBase = new File(baseDir, "volumes");
    File v1f = new File(volDirBase, "v1");
    File v2f = new File(volDirBase, "v2");
    File v3f = new File(volDirBase, "v3");
    File v4f = new File(volDirBase, "v4");
    v1 = new Path("file://" + v1f.getAbsolutePath());
    v2 = new Path("file://" + v2f.getAbsolutePath());
    // v3 not used
    new Path("file://" + v3f.getAbsolutePath());
    v4 = new Path("file://" + v4f.getAbsolutePath());

    // this property is required
    systemPreferredVolumes = v1.toString() + "," + v2.toString();
    siteConfig.put(PreferredVolumeChooser.PREFERRED_VOLUMES_CUSTOM_KEY, systemPreferredVolumes); // exclude v4
    cfg.setSiteConfig(siteConfig);

    siteConfig.put(Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + "logger.volume.chooser", PreferredVolumeChooser.class.getName());
    // deliberately excluded logger preferred volumes

    cfg.setSiteConfig(siteConfig);

    // Only add volumes 1, 2, and 4 to the list of instance volumes to have one volume that isn't in the options list when they are choosing
    cfg.setProperty(Property.INSTANCE_VOLUMES, v1.toString() + "," + v2.toString() + "," + v4.toString());

    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());

    super.configure(cfg, hadoopCoreSite);

  }

  @Test
  public void waLogsFallsBackToSystemDefaultVolumes() throws Exception {
    log.info("Starting waLogsSentToSystemDefaultVolumes");

    Connector connector = getConnector();
    String tableName = "anotherTable";
    connector.tableOperations().create(tableName);

    VolumeChooserIT.addSplits(connector, tableName);
    VolumeChooserIT.writeDataToTable(connector, tableName);
    // should only go to v2 as per configuration in configure()
    VolumeChooserIT.verifyWaLogVolumes(connector, new Range(), systemPreferredVolumes);
  }
}
