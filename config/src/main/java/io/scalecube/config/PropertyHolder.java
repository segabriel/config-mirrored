package io.scalecube.config;

public interface PropertyHolder {

  boolean contains(String name);

  ConfigProperty getConfigProperty(String name);

  PropertyCallback putIfAbsent(String name, Class<?> propertyClass, PropertyCallback propertyCallback);

  PropertyCallback getPropertyCallback(String name, Class<?> propertyClass);

  PropertyCallback getPropertyCallback(Class<?> propertyClass);
}
