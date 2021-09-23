# mysql-to-elasticsearch-java

This project is used to load initial data from mysql to Elasticsearch.

* change the mysql database configuration and elasticsearch configuration in application.properties file to fit your needs.

* Run project as spring boot project.

* Things that you might want to change.

1. Adjust the sql query in PropertyDAOImp.get(...)
2. If run with java, adjust params in run-with-java.sh with the right mysql config params and ES config params for database and elasticsearch server connections
3. If run with maven, adjust params in run-with-maven.sh with the right mysql config params and ES config params for database and elasticsearch server connections

* Run app as a java app
run app with run-with-java.sh
