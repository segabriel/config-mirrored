package io.scalecube.config.examples;

import io.scalecube.config.ConfigRegistry;
import io.scalecube.config.ConfigRegistrySettings;
import io.scalecube.config.StringConfigProperty;
import io.scalecube.config.audit.Slf4JConfigEventListener;
import io.scalecube.config.keyvalue.KeyValueConfigName;
import io.scalecube.config.keyvalue.KeyValueConfigSource;
import io.scalecube.config.zookeeper.ZookeeperConfigConnector;
import io.scalecube.config.zookeeper.ZookeeperConfigRepository;

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
    ZookeeperConfigConnector connector = ZookeeperConfigConnector.forUri("192.168.99.100:2181").build();
    String configSourceCollectionName = "ZookeeperConfigRootPath";

    ZookeeperConfigRepository repository = new ZookeeperConfigRepository(connector);

//    init(repository);

    KeyValueConfigSource zookeeperConfigSource = KeyValueConfigSource
        .withRepository(repository, configSourceCollectionName)
        .groups("group1", "group2", "group3")
        .build();

    ConfigRegistry configRegistry = ConfigRegistry.create(
        ConfigRegistrySettings.builder()
            .addLastSource("ZookeeperConfig", zookeeperConfigSource)
            .addListener(new Slf4JConfigEventListener())
            .keepRecentConfigEvents(3)
            .reloadIntervalSec(1)
            .build());

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

  }

  private static void init(ZookeeperConfigRepository repository) throws Exception {
    repository.put("/group1/ZookeeperConfigRootPath/prop1", "prop_value_1");
    repository.put("/group2/ZookeeperConfigRootPath/prop2", "prop_value_2");
    repository.put("/group3/ZookeeperConfigRootPath/prop3", "prop_value_3");
    repository.put("/ZookeeperConfigRootPath/propRoot", "prop_value_Root");
  }
}
