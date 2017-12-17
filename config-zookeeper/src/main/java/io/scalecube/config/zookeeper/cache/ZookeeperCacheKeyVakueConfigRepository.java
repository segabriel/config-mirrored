package io.scalecube.config.zookeeper.cache;

import io.scalecube.config.keyvalue.KeyValueConfigEntity;
import io.scalecube.config.keyvalue.KeyValueConfigName;
import io.scalecube.config.keyvalue.KeyValueConfigRepository;
import io.scalecube.config.utils.ThrowableUtil;
import io.scalecube.config.zookeeper.ZookeeperConfigConnector;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class ZookeeperCacheKeyVakueConfigRepository implements KeyValueConfigRepository {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperCacheKeyVakueConfigRepository.class);

  private static final ScheduledExecutorService reloadExecutor = reloadExecutor();


  private static ScheduledExecutorService reloadExecutor() {
    ThreadFactory threadFactory = r -> {
      Thread thread = new Thread(r);
      thread.setDaemon(true);
      thread.setName("zookeeper-cache-reloader");
      thread.setUncaughtExceptionHandler((t, e) -> LOGGER.error("Exception occurred: " + e, e));
      return thread;
    };
    return Executors.newSingleThreadScheduledExecutor(threadFactory);
  }

  private final CuratorFramework client;
  private final Duration delay;

  private Map<KeyValueConfigName, TreeNode> roots = new ConcurrentHashMap<>();
  private Map<String, KeyValueConfigEntity> props = new ConcurrentHashMap<>();

  public ZookeeperCacheKeyVakueConfigRepository(@Nonnull ZookeeperConfigConnector connector, @Nonnull Duration delay) {
    this.client = Objects.requireNonNull(connector).getClient();
    this.delay = delay;
  }

  @Override
  public List<KeyValueConfigEntity> findAll(@Nonnull KeyValueConfigName configName) throws Exception {
    roots.computeIfAbsent(configName, TreeNode::new);
    System.out.println(props);
    return new ArrayList<>(props.values());
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
        throw ThrowableUtil.propagate(e);
//      handleException(e);
      }
    }

    @Override
    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
//      Stat newStat = event.getStat();
      if (event.getData() != null) {
        System.err.println("processResult: " + event + "      8*****> data => " + new String(event.getData()));
      } else {
        System.err.println("processResult: " + event);
      }


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
                .sorted() //todo Present new children in sorted order for test determinism. WTF?
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
              KeyValueConfigEntity entity = entity(event);
              props.put(entity.getPropName(), entity);
            }
          } else if (event.getResultCode() == KeeperException.Code.NONODE.intValue()) {
            wasDeleted();
          }
          break;
        default:
          // An unknown event, probably an error of some sort like connection loss.
//        LOG.info(String.format("Unknown event %s", event));
          return;
      }
    }

    private String resolvePath(KeyValueConfigName configName) {
      StringBuilder sb = new StringBuilder("/");
      configName.getGroupName().ifPresent(groupName -> sb.append(groupName).append("/"));
      sb.append(configName.getCollectionName());
      return sb.toString();
    }

    private void refresh() {
      refreshChildren();
      refreshData();
    }

    private void refreshChildren() {
      try {
        client.getChildren().usingWatcher(this).inBackground(this).forPath(path);
      } catch (Exception e) {
        throw ThrowableUtil.propagate(e);
      }
    }

    private void refreshData() {
      try {
        byte[] bytes = client.getData().usingWatcher(this)/*.inBackground(this)*/.forPath(path);
        if (bytes != null) {
          System.err.println("!!! =>>>>>>>>>>>>>>>>>>> " + new String(bytes));
        }
      } catch (Exception e) {
        throw ThrowableUtil.propagate(e);
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
        throw ThrowableUtil.propagate(e);
      }
    }

    private KeyValueConfigEntity entity(CuratorEvent event) {
      KeyValueConfigEntity entity = new KeyValueConfigEntity().setConfigName(configName);
      entity.setPropName(propertyName);
      entity.setPropValue(new String(event.getData()));
      return entity;
    }

    private KeyValueConfigEntity entity(byte[] data) {
      KeyValueConfigEntity entity = new KeyValueConfigEntity().setConfigName(configName);
      entity.setPropName(propertyName);
      entity.setPropValue(new String(data));
      return entity;
    }

    private String propertyName(String fullPath) {
      String rootPath = resolvePath(configName);
      return fullPath.substring(rootPath.length() + 1).replace("/", ".");
    }
  }

}
