package org.spring.cassandra.example;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cassandra.core.AsynchronousQueryListener;
import org.springframework.cassandra.core.RowMapper;
import org.springframework.cassandra.core.keyspace.DropTableSpecification;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.util.Assert;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Truncate;
import com.datastax.driver.core.querybuilder.Update;

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

	private void demoAdd() {

		LOG.info("Insert Cassandra Demo");

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

	private void demoUpdate() {

		LOG.info("Update Cassandra Demo");

		cassandraOperations.update(new Person("123123123", "Alison", 35));

		Update update = QueryBuilder.update("person");
		update.setConsistencyLevel(ConsistencyLevel.ONE);
		update.with(QueryBuilder.set("age", 35));
		update.where(QueryBuilder.eq("id", "123123123"));

		cassandraOperations.execute(update);

		String cql = "update person set age = 35 where id = '123123123'";

		cassandraOperations.execute(cql);

	}

	private void demoDelete() {

		LOG.info("Delete Cassandra Demo");

		cassandraOperations.delete(new Person("123123123", null, 0));

		Delete delete = QueryBuilder.delete().from("person");
		delete.where(QueryBuilder.eq("id", "123123123"));

		cassandraOperations.execute(delete);

		String cql = "delete from person where id = '123123123'";

		cassandraOperations.execute(cql);

	}

	private void demoTruncate() {

		LOG.info("Truncate Cassandra Demo");

		cassandraOperations.truncate("person");

		Truncate truncate = QueryBuilder.truncate("person");

		cassandraOperations.execute(truncate);

		String cql = "truncate person";

		cassandraOperations.execute(cql);

	}

	private void demoQuery() {

		LOG.info("Query Cassandra Demo");

		String cqlAll = "select * from person";

		try {
			List<Person> results = cassandraOperations.select(cqlAll, Person.class);
			for (Person p : results) {
				LOG.info(String.format("Found People with Name [%s] for id [%s]", p.getName(), p.getId()));
			}
		} finally {
		}

		String cqlOne = "select * from person where id = '123123123'";

		try {
			Person p = cassandraOperations.selectOne(cqlOne, Person.class);
			LOG.info(String.format("Found Person with Name [%s] for id [%s]", p.getName(), p.getId()));
		} finally {
		}

		try {
			Select select = QueryBuilder.select().from("person");
			select.where(QueryBuilder.eq("id", "123123123"));

			Person p = cassandraOperations.selectOne(select, Person.class);
			LOG.info(String.format("Found Person with Name [%s] for id [%s]", p.getName(), p.getId()));
		} finally {
		}

		List<Person> results = cassandraOperations.query(cqlAll, new RowMapper<Person>() {

			public Person mapRow(Row row, int rowNum) throws DriverException {
				Person p = new Person(row.getString("id"), row.getString("name"), row.getInt("age"));
				return p;
			}
		});

		for (Person p : results) {
			LOG.info(String.format("Found People with Name [%s] for id [%s]", p.getName(), p.getId()));
		}

	}

	private void demoExecute() {

		LOG.info("Excecute Cassandra Demo");

		cassandraOperations.executeAsynchronously("delete from person where id = '123123123'",
				new AsynchronousQueryListener() {

					public void onQueryComplete(ResultSetFuture rsf) {
						LOG.info("Async Query Completed");
					}
				});

		cassandraOperations.execute("create table test_table (id uuid primary key, event text)");

		DropTableSpecification dropper = DropTableSpecification.dropTable("test_table");
		cassandraOperations.execute(dropper);

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
		cass.demoAdd();
		cass.demoQuery();
		cass.demoUpdate();
		cass.demoExecute();
		cass.demoDelete();
		cass.demoTruncate();
		cass.destroy();

		System.exit(0);

	}
}
