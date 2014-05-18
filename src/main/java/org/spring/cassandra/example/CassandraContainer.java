package org.spring.cassandra.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.util.Assert;

public class CassandraContainer {

  @Autowired
  @Qualifier("myTemplateId")
  private static final Logger LOG = LoggerFactory.getLogger(CassandraContainer.class);

  private static ApplicationContext applicationContext;
  private static CassandraOperations cassandraOperations;

  private void init() {

    applicationContext = new ClassPathXmlApplicationContext("/cassandra-spring.xml");

  }

  private void initAnnotationConfig() {

    applicationContext = new AnnotationConfigApplicationContext("org.spring.cassandra.example.config");

  }

  private void initBeans() {

    cassandraOperations = applicationContext.getBean("cassandraTemplate", CassandraOperations.class);

    Assert.notNull(cassandraOperations, "CassandraOperations is null.");

  }

  private void destroy() {
    applicationContext = null;
  }

  private void demo() {

    LOG.info("Adding a Person to Cassandra");

    cassandraOperations.insert(new Person("123123123", "Alison", 39));

  }

  private void showBeans() {

    String[] names = applicationContext.getBeanDefinitionNames();

    for (String n : names) {
      LOG.info(n);
    }

  }

  /**
   * @param args
   */
  public static void main(String[] args) {

    CassandraContainer cass = new CassandraContainer();
    cass.initAnnotationConfig();
    cass.initBeans();
    cass.showBeans();
    cass.demo();
    cass.destroy();

    System.exit(0);

  }
}
