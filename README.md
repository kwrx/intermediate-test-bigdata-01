# Report

## Task 01
See ```task01/src/main/java/Task01.java```

## Task 02

Create Database:
```sql
CREATE DATABASE 'task02';
USE 'task02';
```

Create external table for surveys json:
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS survey (
    name string, 
    surname string, 
    age int,
    livingPlace struct<city: string, province: string>, 
    skills array<struct<name: string, description: string, level: int>>
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE LOCATION '/user/root/surveys';
```

Create schema:
```sql
CREATE TEMPORARY TABLE IF NOT EXISTS city
    AS SELECT DISTINCT livingPlace.city AS name, 
                       livingPlace.province AS province
       FROM survey;

CREATE TEMPORARY TABLE IF NOT EXISTS skills_exploded
    AS SELECT explode(skills) AS skill
       FROM survey;

CREATE TEMPORARY TABLE IF NOT EXISTS skills
    AS SELECT DISTINCT skill.name AS name, 
                       skill.description AS description,
                       skill.level AS level
       FROM skills_exploded;

CREATE TEMPORARY TABLE IF NOT EXISTS entity 
    AS SELECT name, surname, age, city.name AS city, city.province AS province, skills.name AS skill, skills.level AS level
       FROM survey, city, skills
       WHERE survey.livingPlace.city = city.name AND survey.livingPlace.provice = city.province AND skills.name IN (SELECT name FROM skills);
```

See ```task02/queries.sql``` for queries.

    
## Task 03

First export data from Hive to CSV:

```sql
INSERT OVERWRITE LOCAL DIRECTORY '/user/root/survey.csv' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT * FROM survey;
```

See ```task03/src/main/java/Task03.java``` to see code.  

Build with:
```bash
./gradlew jar
```

and run with:
```bash
hadoop jar task03-1.0-SNAPSHOT.jar Task03 /user/root/survey.csv /user/root/output
```