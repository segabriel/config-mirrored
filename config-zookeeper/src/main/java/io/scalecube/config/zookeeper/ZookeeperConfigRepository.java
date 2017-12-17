package io.scalecube.config.zookeeper;

import io.scalecube.config.keyvalue.KeyValueConfigEntity;
import io.scalecube.config.keyvalue.KeyValueConfigName;
import io.scalecube.config.keyvalue.KeyValueConfigRepository;
import io.scalecube.config.utils.ThrowableUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ZookeeperConfigRepository implements KeyValueConfigRepository {
  private final ZookeeperConfigConnector connector;

  public ZookeeperConfigRepository(@Nonnull ZookeeperConfigConnector connector) {
    this.connector = Objects.requireNonNull(connector);
  }

  @Override
  public List<KeyValueConfigEntity> findAll(@Nonnull KeyValueConfigName configName) throws Exception {
    List<KeyValueConfigEntity> entities = new ArrayList<>();
    List<String> propertyNames = connector.getClient().getChildren().forPath(resolvePath(configName));
    for (String propertyName : propertyNames) {
      entities.add(find(configName, propertyName));
    }
    return entities;
  }

  public void put(String path, String value) throws Exception {
    byte[] data = value.getBytes(StandardCharsets.UTF_8);
    try {
      connector.getClient().create().creatingParentsIfNeeded().forPath(path, data);
    } catch (KeeperException.NodeExistsException e) {
      // key already exists - update the data instead
      connector.getClient().setData().forPath(path, data);
    }
  }

  public void put(Map<String, String> props) throws Exception {
    CuratorFramework client = connector.getClient();

    client.transaction().forOperations(props.entrySet().stream()
        .map(entry -> {
          try {
            String path = entry.getKey();
            byte[] data = entry.getValue().getBytes(StandardCharsets.UTF_8);
//            client.create().creatingParentsIfNeeded().forPath(path);
            return client.transactionOp().setData().forPath(path, data);
          } catch (Exception e) {
            throw ThrowableUtil.propagate(e);
          }
        }).collect(Collectors.toList()));
  }

  private KeyValueConfigEntity find(KeyValueConfigName configName, String propertyName) throws Exception {
    byte[] data = connector.getClient().getData().forPath(resolvePath(configName, propertyName));
    String value = new String(data, StandardCharsets.UTF_8);
    KeyValueConfigEntity entity = new KeyValueConfigEntity().setConfigName(configName);
    entity.setPropName(propertyName);
    entity.setPropValue(value);
    entity.setDisabled(false);
    return entity;
  }

  private String resolvePath(KeyValueConfigName configName) {
    StringBuilder sb = new StringBuilder("/");
    configName.getGroupName().ifPresent(groupName -> sb.append(groupName).append("/"));
    sb.append(configName.getCollectionName());
    return sb.toString();
  }

  private String resolvePath(KeyValueConfigName configName, String propertyName) {
    return resolvePath(configName) + "/" + propertyName;
  }
}
