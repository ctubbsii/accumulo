package org.apache.accumulo.server.tables;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.ZooKeeperMapping;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
public class TestZKImpl {
    public static void main(String[] args) {
        try {
            // Initialize ServerContext and ZooReaderWriter
            ZooKeeper zoo = new ZooKeeper("localhost:2181/", 2000, null);
            File file = new File("/home/ubuntu/Projects/fluo-uno/install/accumulo-2.1.3/conf/accumulo.properties");
            var siteConfig = SiteConfiguration.fromFile(file).build();
            ServerContext context = new ServerContext(siteConfig);
            ZooReaderWriter zooReaderWriter = new ZooReaderWriter(siteConfig);

            String zPath = Constants.ZROOT + "/" + context.getInstanceID() + Constants.ZNAMESPACES;

            // Initialize the namespace map in ZooKeeper
            ZooKeeperMapping.initializeNamespaceMap(zooReaderWriter, zPath);

            // Verify that the JSON data is created and stored in ZooKeeper
            byte[] initialData = zoo.getData(zPath, false, null);
            if (initialData != null) {
                System.out.println("Initial namespace map in ZooKeeper: " + new String(initialData, UTF_8));
            } else {
                System.out.println("No initial data found at path: " + zPath);
            }

            // Create a dummy namespace ID and name for testing
            NamespaceId namespaceId = NamespaceId.of("testNamespaceId");
            String namespaceName = "TestNamespace";

            // Prepare the new namespace state and append to ZooKeeper
            TableManager.prepareNewNamespaceState(context, namespaceId, namespaceName, ZooUtil.NodeExistsPolicy.OVERWRITE);

            // Retrieve the updated namespace map from ZooKeeper
            byte[] updatedData = zoo.getData(zPath, false, null);
            if (updatedData != null) {
                System.out.println("Updated namespace map in ZooKeeper: " + new String(updatedData, UTF_8));
            } else {
                System.out.println("No updated data found at path: " + zPath);
            }

            // Test getting all namespaces using the getNamespaceMap method
            Map<String, String> namespaceMap = ZooKeeperMapping.getNamespaceMap(context.getZooCache(), zPath);
            System.out.println("Namespaces retrieved using getNamespaceMap: " + namespaceMap);

            // Test getAllNamespaces with ZcStat to ensure it only gets changes
//            ZooCache.ZcStat stat = new ZooCache.ZcStat();
//            List<String> namespaces = context.getZooCache().getChildren(zPath, stat);
            //        if (stat.getMzxid() != 0) {
            //            System.out.println("Namespaces retrieved with changes detected: " + namespaces);
            //        } else {
            //            System.out.println("No changes detected in ZooKeeper.");
            //        }
        } catch (KeeperException | InterruptedException | IOException | IllegalArgumentException e) {
            e.printStackTrace();
        }
    }
}