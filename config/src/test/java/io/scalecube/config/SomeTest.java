package io.scalecube.config;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.scalecube.config.source.ConfigSource;
import org.jasypt.util.text.BasicTextEncryptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static io.scalecube.config.TestUtil.mapBuilder;
import static io.scalecube.config.TestUtil.newConfigRegistry;
import static io.scalecube.config.TestUtil.toConfigProps;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SomeTest {

  @Mock
  ConfigSource configSource;

  @Test
  public void testIntMultimapProperty() {
    when(configSource.loadConfig())
        .thenReturn(toConfigProps(mapBuilder()
            .put("test", "hello world")
            .build()));

    ConfigRegistryImpl configRegistry = newConfigRegistry(configSource);

    System.out.println(configRegistry.stringProperty("test").valueOrThrow());

  }

  @Test
  public void name() {
    BasicTextEncryptor textEncryptor = new BasicTextEncryptor();
    textEncryptor.setPassword("secretPass");

    String prop = textEncryptor.encrypt("hello world!");

    System.out.println(prop);

    String msg = textEncryptor.decrypt(prop);

    System.out.println(msg);

  }
}

