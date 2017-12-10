package io.scalecube.config;

import io.scalecube.config.utils.ThrowableUtil;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @param <T> type of the property value
 */
class ObjectConfigPropertyImpl<T> extends AbstractConfigProperty<T> implements ObjectConfigProperty<T> {

  ObjectConfigPropertyImpl(Map<String, String> bindingMap, Class<T> cfgClass, PropertyHolder propertyHolder) {
    super(cfgClass.getName(), cfgClass);

    List<ObjectPropertyField> propertyFields = toPropertyFields(bindingMap, cfgClass);
    setPropertyCallback(computePropertyCallback(cfgClass, propertyFields, propertyHolder));

    computeValue(propertyFields.stream()
        .map(ObjectPropertyField::getPropertyName)
        .filter(propertyHolder::contains)
        .map(propertyHolder::getConfigProperty)
        .collect(Collectors.toList()));
  }

  @Override
  public T value(T defaultValue) {
    return value().orElse(defaultValue);
  }

  private List<ObjectPropertyField> toPropertyFields(Map<String, String> bindingMap, Class<T> cfgClass) {
    List<ObjectPropertyField> propertyFields = new ArrayList<>(bindingMap.size());
    for (String fieldName : bindingMap.keySet()) {
      Field field;
      try {
        field = cfgClass.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
        throw ThrowableUtil.propagate(e);
      }
      int modifiers = field.getModifiers();
      if (!Modifier.isStatic(modifiers) && !Modifier.isFinal(modifiers)) {
        propertyFields.add(new ObjectPropertyField(field, bindingMap.get(fieldName)));
      }
    }
    return propertyFields;
  }

  private PropertyCallback<T> computePropertyCallback(Class<T> cfgClass,
      List<ObjectPropertyField> propertyFields,
      PropertyHolder propertyHolder) {

    PropertyCallback<T> propertyCallback =
        new PropertyCallback<>(list -> ObjectPropertyParser.parseObject(list, propertyFields, cfgClass));

    // ensure that only one propertyCallback instance will be shared among instances of the same type
    synchronized (propertyHolder) {
      //noinspection unchecked
      return propertyFields.stream()
          .map(ObjectPropertyField::getPropertyName)
          .map(propName -> propertyHolder.putIfAbsent(propName, propertyClass, propertyCallback))
          .collect(Collectors.toSet())
          .iterator()
          .next();
    }
  }
}
