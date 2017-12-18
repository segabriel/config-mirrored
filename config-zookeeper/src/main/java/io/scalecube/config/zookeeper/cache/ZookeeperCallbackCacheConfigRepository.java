package io.scalecube.config.zookeeper.cache;

import io.scalecube.config.keyvalue.KeyValueConfigEntity;
import io.scalecube.config.keyvalue.KeyValueConfigName;
import io.scalecube.config.keyvalue.KeyValueConfigRepository;
import io.scalecube.config.zookeeper.ZookeeperConfigConnector;
import io.scalecube.config.zookeeper.ZookeeperSimpleConfigRepository;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class ZookeeperCallbackCacheConfigRepository implements KeyValueConfigRepository {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperCallbackCacheConfigRepository.class);

  private final CuratorFramework client;

  private Map<KeyValueConfigName, TreeNode> roots = new ConcurrentHashMap<>();
  private Map<KeyValueConfigName, Map<String, KeyValueConfigEntity>> props = new ConcurrentHashMap<>();

  public ZookeeperCallbackCacheConfigRepository(@Nonnull ZookeeperConfigConnector connector) {
    this.client = Objects.requireNonNull(connector).getClient();
  }

  @Override
  public List<KeyValueConfigEntity> findAll(@Nonnull KeyValueConfigName configName) {
    roots.computeIfAbsent(configName, TreeNode::new);
    ArrayList<KeyValueConfigEntity> result = new ArrayList<>(props.computeIfAbsent(configName, k -> new ConcurrentHashMap<>()).values());
    System.out.println("findAll = " + result);
    return result;
  }

  private void warmup(KeyValueConfigName configName) {
    try {
      new ZookeeperSimpleConfigRepository(client).findAll(configName)
          .forEach(entity -> props.computeIfAbsent(configName, k -> new ConcurrentHashMap<>()).put(entity.getPropName(), entity));
    } catch (Exception e) {
      LOGGER.error("Exception occurred: " + e, e);
    }
  }

  private final class TreeNode implements Watcher, BackgroundCallback {

    private final KeyValueConfigName configName;
    private final String path;
    private final String propertyName;
    private final TreeNode parent;
    private final Map<String, TreeNode> children = new ConcurrentHashMap<>();

    private TreeNode(KeyValueConfigName configName) {
      this.configName = configName;
      this.path = resolvePath(configName);
      this.propertyName = "";
      this.parent = null;
      warmup(configName);
      refresh();
    }

    private TreeNode(KeyValueConfigName configName, String path, String propertyName, TreeNode parent) {
      this.configName = configName;
      this.path = path;
      this.propertyName = propertyName;
      this.parent = parent;
    }

    private TreeNode newChildNode(String fullPath) {
      return new TreeNode(configName, fullPath, propertyName(fullPath), this);
    }

    @Override
    public void process(WatchedEvent event) {
      try {
        switch (event.getType()) {
          case NodeCreated:
            Preconditions.checkState(parent == null, "unexpected NodeCreated on non-root node");
            wasCreated();
            break;
          case NodeChildrenChanged:
            refreshChildren();
            break;
          case NodeDataChanged:
            refreshData();
            break;
          case NodeDeleted:
            wasDeleted();
            break;
        }
      } catch (Exception e) {
        LOGGER.error("Exception occurred: " + e, e);
      }
    }

    @Override
    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
//      if (event.getData() != null) {
//        System.err.println("******************** processResult: " + event + "      8*****> data => " + new String(event.getData()));
//      } else {
//        System.err.println("******************** processResult: " + event);
//      }
      switch (event.getType()) {
        case EXISTS:
          Preconditions.checkState(parent == null, "unexpected EXISTS on non-root node");
          if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
            wasCreated();
          }
          break;
        case CHILDREN:
          if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
            event.getChildren().stream()
                .filter(child -> !children.containsKey(child))
                .forEach(child -> {
                  String fullPath = ZKPaths.makePath(path, child);
                  TreeNode node = newChildNode(fullPath);
                  if (children.putIfAbsent(child, node) == null) {
                    node.wasCreated();
                  }
                });
          } else if (event.getResultCode() == KeeperException.Code.NONODE.intValue()) {
            wasDeleted();
          }
          break;
        case GET_DATA:
          if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
            if (event.getData() != null && event.getData().length > 0) {
              KeyValueConfigEntity entity = entity(event.getData());
              props.computeIfAbsent(configName, k -> new ConcurrentHashMap<>()).put(entity.getPropName(), entity);
            }
          } else if (event.getResultCode() == KeeperException.Code.NONODE.intValue()) {
            wasDeleted();
          }
          break;
        default:
          LOGGER.warn("Unknown event: " + event);
      }
    }

    private String resolvePath(KeyValueConfigName configName) {
      return configName.getGroupName().map(group -> "/" + group + "/").orElse("/") + configName.getCollectionName();
    }

    private void refresh() {
      refreshChildren();
      refreshData();
    }

    private void refreshChildren() {
      try {
        client.getChildren().usingWatcher(this).inBackground(this).forPath(path);
      } catch (Exception e) {
        LOGGER.error("Exception occurred: " + e, e);
      }
    }

    private void refreshData() {
      try {
        client.getData().usingWatcher(this).inBackground(this).forPath(path);
      } catch (Exception e) {
        LOGGER.error("Exception occurred: " + e, e);
      }
    }

    private void wasReconnected() {
      refresh();
      children.values().forEach(TreeNode::wasReconnected);
    }

    private void wasCreated() {
      refresh();
    }

    private void wasDeleted() {
      try {
        props.remove(propertyName);
        children.forEach((path, child) -> child.wasDeleted());
        children.clear();
        if (parent == null) { // is root node?
          client.checkExists().usingWatcher(this).inBackground(this).forPath(path);
        } else {
          parent.children.remove(ZKPaths.getNodeFromPath(path), this);
        }
      } catch (Exception e) {
        LOGGER.error("Exception occurred: " + e, e);
      }
    }

    private KeyValueConfigEntity entity(byte[] data) {
      KeyValueConfigEntity entity = new KeyValueConfigEntity().setConfigName(configName);
      entity.setPropName(propertyName);
      entity.setPropValue(new String(data));
      return entity;
    }

    private String propertyName(String fullPath) {
      String rootPath = resolvePath(configName);
      return fullPath.substring(rootPath.length() + "/".length()).replace("/", ".");
    }
  }
}
