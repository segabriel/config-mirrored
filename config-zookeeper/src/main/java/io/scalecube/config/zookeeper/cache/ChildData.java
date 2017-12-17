package io.scalecube.config.zookeeper.cache;

import org.apache.curator.utils.PathUtils;
import org.apache.zookeeper.data.Stat;

public class ChildData {
    private final String path;
    private final Stat stat;
    private final String data;

    public ChildData(String path, Stat stat, String data) {
        this.path = PathUtils.validatePath(path);
        this.stat = stat;
        this.data = data;
    }

    /**
     * Returns the full path of the this child
     *
     * @return full path
     */
    public String getPath()
    {
        return path;
    }

    /**
     * Returns the stat data for this child
     *
     * @return stat or null
     */
    public Stat getStat()
    {
        return stat;
    }

    /**
     * <p>Returns the node data for this child</p>
     *
     * @return node data or null
     */
    public String getData()
    {
        return data;
    }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ChildData childData = (ChildData) o;

    if (path != null ? !path.equals(childData.path) : childData.path != null) return false;
    if (stat != null ? !stat.equals(childData.stat) : childData.stat != null) return false;
    return data != null ? data.equals(childData.data) : childData.data == null;
  }

  @Override
  public int hashCode() {
    int result = path != null ? path.hashCode() : 0;
    result = 31 * result + (stat != null ? stat.hashCode() : 0);
    result = 31 * result + (data != null ? data.hashCode() : 0);
    return result;
  }
}
