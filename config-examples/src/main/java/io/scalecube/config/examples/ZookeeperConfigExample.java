package io.scalecube.config.examples;

import com.google.common.collect.ImmutableMap;
import io.scalecube.config.ConfigRegistry;
import io.scalecube.config.ConfigRegistrySettings;
import io.scalecube.config.StringConfigProperty;
import io.scalecube.config.audit.Slf4JConfigEventListener;
import io.scalecube.config.keyvalue.KeyValueConfigName;
import io.scalecube.config.keyvalue.KeyValueConfigSource;
import io.scalecube.config.zookeeper.ZookeeperConfigConnector;
import io.scalecube.config.zookeeper.ZookeeperConfigRepository;
import io.scalecube.config.zookeeper.cache.ZookeeperCacheKeyVakueConfigRepository;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.time.Duration;
import java.util.Map;

/**
 * <p>For program properly functioning add some data to zookeeper</p>
 *
 * <p>First, you should have some config path, it's like a
 * [{@link KeyValueConfigName#groupName}]/{@link KeyValueConfigName#collectionName}.
 * This example will be searching for properties in ['/ZookeeperConfigRootPath', '/group1/ZookeeperConfigRootPath',
 * '/group2/ZookeeperConfigRootPath', '/group3/ZookeeperConfigRootPath'] yours Zookeeper instance.</p>
 * <p>Second, all property names should belong specified above config path and contain some value.
 * This example will be searching for following property paths: </p>
 *  <ul>
 *    <li>/ZookeeperConfigRootPath/propRoot -> prop_value_root</li>
 *    <li>/group1/ZookeeperConfigRootPath/prop1 -> prop_value_1</li>
 *    <li>/group2/ZookeeperConfigRootPath/prop2 -> prop_value_2</li>
 *    <li>/group3/ZookeeperConfigRootPath/prop3 -> prop_value_3</li>
 *  </ul>
 *  <p>If you want, you can use {@link #init(ZookeeperConfigRepository)} before to fill your Zookeeper instance</p>
 *  <p>NOTICE: if you are concerned about the consistency of changing your properties use implementation with
 *  a transaction, for example {@link ZookeeperConfigRepository#put(Map)}</p>
 */
public class ZookeeperConfigExample {

  public static void main(String[] args) throws Exception {
    ZookeeperConfigConnector connector = ZookeeperConfigConnector.forUri("localhost:2181").build();
    String configSourceCollectionName = "ZookeeperConfigRootPath";

    CuratorFramework client = connector.getClient();
    q1(client);

//    ZookeeperConfigRepository repository = new ZookeeperConfigRepository(connector);
    ZookeeperCacheKeyVakueConfigRepository repository2 = new ZookeeperCacheKeyVakueConfigRepository(connector, Duration.ZERO);

    ZookeeperConfigRepository repository = new ZookeeperConfigRepository(connector);
//    init(repository);

    KeyValueConfigSource zookeeperConfigSource = KeyValueConfigSource
        .withRepository(repository2, configSourceCollectionName)
        .groups("group1", "group2", "group3")
        .build();

    ConfigRegistry configRegistry = ConfigRegistry.create(
        ConfigRegistrySettings.builder()
            .addLastSource("ZookeeperConfig", zookeeperConfigSource)
            .addListener(new Slf4JConfigEventListener())
            .keepRecentConfigEvents(3)
            .reloadIntervalSec(1)
            .build());

    Thread.sleep(5000);

    StringConfigProperty prop1 = configRegistry.stringProperty("prop1");
    System.out.println("### Initial zookeeper config property: prop1=" + prop1.value().get() +
        ", group=" + prop1.origin().get());

    StringConfigProperty prop2 = configRegistry.stringProperty("prop2");
    System.out.println("### Initial zookeeper config property: prop2=" + prop2.value().get() +
        ", group=" + prop2.origin().get());

    StringConfigProperty prop3 = configRegistry.stringProperty("prop3");
    System.out.println("### Initial zookeeper config property: prop3=" + prop3.value().get() +
        ", group=" + prop3.origin().get());

    StringConfigProperty propRoot = configRegistry.stringProperty("propRoot");
    System.out.println("### Initial zookeeper config **root** property: propRoot=" + propRoot.value().get() +
        ", group=" + propRoot.origin().get());

    client.getCuratorListenable().addListener(new CuratorListener() {
      @Override
      public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
        System.err.println("eventReceived: " + event);
      }
    });

//    client.getChildren().usingWatcher(new Watcher() {
//      @Override
//      public void process(WatchedEvent event) {
//        System.err.println("WatchedEvent: " + event);
//        System.out.println();
//      }
//    }).forPath("/");
//
//    client.getData().usingWatcher(new Watcher() {
//      @Override
//      public void process(WatchedEvent event) {
//        System.err.println("WatchedEvent0: " + event);
//        System.out.println();
//      }
//    }).forPath("/");

    client.getData().usingWatcher(new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        System.err.println(System.currentTimeMillis() + "WatchedEvent2: " + event);
      }
    }).forPath("/group2/ZookeeperConfigRootPath/prop2");

    client.getData().usingWatcher(new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        System.err.println(System.nanoTime() + "WatchedEvent3: " + event);
      }
    }).forPath("/group3/ZookeeperConfigRootPath/prop3");


//    Thread.sleep(5000);
//
//    repository.put("/group2/ZookeeperConfigRootPath/prop2", "prop_value_233");


//    Thread.sleep(5000);

    repository.put(ImmutableMap.<String, String>builder()
        .put("/group2/ZookeeperConfigRootPath/prop2", "new2")
        .put("/group3/ZookeeperConfigRootPath/prop3", "new3")
        .build());

    client.getChildren().usingWatcher(new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        System.err.println(System.nanoTime() + "WatchedEventChild1: " + event);
      }
    }).forPath("/ZookeeperConfigRootPath");
    client.getChildren().usingWatcher(new CuratorWatcher() {
      @Override
      public void process(WatchedEvent event) throws Exception {
        System.err.println(System.nanoTime() + "WatchedEventChild2: " + event);
      }
    }).forPath("/ZookeeperConfigRootPath");


    repository.put("/ZookeeperConfigRootPath/propRoot/sub/sub2", "prop_value_SubRoot");
//    repository.put("/ZookeeperConfigRootPath/sub", "prop_value_SubRoot");

    System.err.println(client.getChildren().forPath("/ZookeeperConfigRootPath"));


    Thread.sleep(10000);

  }

  private static void q1(CuratorFramework client) throws Exception {
//    TreeCache cache = new TreeCache(client, "/ZookeeperConfigRootPath");
//    cache.start();
//    cache.getListenable().addListener(new TreeCacheListener() {
//
//      @Override
//      public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
//        System.err.println("PathChildrenCacheEvent0: " + event);
//        if (event.getData() != null) {
//          System.err.println("PathChildrenCacheEvent1: " + new String(event.getData().getData()));
//        }
//      }
//    });
  }



  private static void init(ZookeeperConfigRepository repository) throws Exception {
    repository.put("/group1/ZookeeperConfigRootPath/prop1", "prop_value_1");
    repository.put("/group2/ZookeeperConfigRootPath/prop2", "prop_value_2");
    repository.put("/group3/ZookeeperConfigRootPath/prop3", "prop_value_3");
    repository.put("/ZookeeperConfigRootPath/propRoot", "prop_value_Root");
  }
}
