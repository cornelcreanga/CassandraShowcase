package org.spring.cassandra.example.repo;

import org.spring.cassandra.example.Person;
import org.springframework.data.cassandra.repository.TypedIdCassandraRepository;

public interface PersonRepository extends TypedIdCassandraRepository<Person, String> {

}
