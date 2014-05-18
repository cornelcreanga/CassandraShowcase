package org.spring.cassandra.example;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.util.Assert;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

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

    Insert insert = QueryBuilder.insertInto("person");
    insert.setConsistencyLevel(ConsistencyLevel.ONE);
    insert.value("id", "123123123");
    insert.value("name", "Alison");
    insert.value("age", 39);

    cassandraOperations.execute(insert);

    String cql = "insert into person (id, name, age) values ('123123123', 'Alison', 30)";

    cassandraOperations.execute(cql);

    String cqlIngest = "insert into person (id, name, age) values (?, ?, ?)";

    List<Object> person1 = new ArrayList<Object>();
    person1.add("10000");
    person1.add("David");
    person1.add(40);

    List<Object> person2 = new ArrayList<Object>();
    person2.add("10001");
    person2.add("Roger");
    person2.add(65);

    List<List<?>> people = new ArrayList<List<?>>();
    people.add(person1);
    people.add(person2);

    cassandraOperations.ingest(cqlIngest, people);

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
