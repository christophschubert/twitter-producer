# twitter-producer
Sample Apache Kafka producer which consumes tweets and publishes them to a Kafka cluster.


The following contains some sample KSQL queries:

```
show topics;

create stream feed_raw (
  text varchar,
  created_at varchar,
  user struct<
    id bigint, 
    name varchar, 
    screen_name varchar, 
    followers_count integer, 
    created_at varchar
  >,
  entities struct<
    hashtags array<
        struct<
            text varchar, 
            indices array<integer>
        >
    >, 
    user_mentions array<
        struct<
            name varchar, 
            id bigint
        >
    >
  >,
  lang varchar
) with (
  kafka_topic='feed_raw', value_format='json'
);

show streams;

select text from feed_raw emit changes;

select * from  FEED_RAW where lang= 'en' emit changes;

select text, lang from feed_raw where lang='de' emit changes;

create stream feed_restructured as select
  text,
  user,
  created_at,
  entities->hashtags as hashtags,
  entities->user_mentions as user_mentions,
  lang as language
  from feed_raw emit changes;

# show that no topics was created

# query new stream:
select text, hashtags from feed_restructured emit changes;

#select text, hashtags, ARRAY_LENGTH(hashtags) as l  from feed_restructured emit changes;



create stream tags as select user->name, explode(hashtags)->text as tag, language from feed_restructured emit changes;



  create table language_counts as select language, count(*) as count from  FEED_RESTRUCTURED group by  LANGUAGE emit changes;

describe language_counts;

select * from language_counts emit changes;

select lcase(tag), count(*) from tags window tumbling (size 20 seconds) group by lcase(tag) emit changes;

  create table language_descriptions (rowkey varchar  key, name varchar) with (kafka_topic='lang_table', value_format='json', partitions=1);


insert into language_descriptions values ('de', 'Deutsch');
insert into language_descriptions values ('en', 'Englisch');
insert into language_descriptions values ('ru', 'Russisch');


create table language_count_enriched with (kafka_topic='lce', value_format='json') as select d.name, c.count from LANGUAGE_COUNTS c inner join  LANGUAGE_DESCRIPTIONS d on c.rowkey = d.rowkey   emit changes;

```


Different way creating types:
```sql
create type user_mention as struct<name varchar, id bigint>;

create type hash_tag as struct<text varchar,  indices array<integer>>;

create type entity as struct<hashtags array<hash_tag>, user_mentions array<user_mention>>;

create type user as struct<id bigint, 
    name varchar, 
    screen_name varchar, 
    followers_count integer, 
    created_at varchar
>;

create stream feed_raw (
  text varchar,
  created_at varchar,
  user user,
  entities entity,
  lang varchar
) with (
  kafka_topic='feed_raw', value_format='json'
);

select user->name, user->followers_count, entities->hashtags, text from feed_raw emit changes;

```

When we try to add a field which is not there, `null` value will be filled:
```sql
create stream feed_raw_non_existing_fields (
  text varchar,
  created_at varchar,
  user user,
  entities entity,
  lang varchar,
  unknown_text varchar,
  unknown_int integer,
  unknown_struct struct<a varchar, b integer>
) with (
  kafka_topic='feed_raw', value_format='json'
);

select user->name, unknown_text, unknown_int, unknown_struct, text from feed_raw_non_existing_fields emit changes;
```

Now that we have seen how we can de-construct a struct using the `->` syntax, how can we build up a struct?

```
select 
struct(language := lang, user := user->name)  as meta,
text as content
from feed_raw emit changes;
```

create stream summarized_json with (format='JSON') as
select
struct(language := lang, user := user->name)  as meta,
text as content
from feed_raw emit changes;

Observe that a topic with the name in the stream in upper-case and a `null` key was created. 

To add keys, one can add a key modifier:
```sql
create stream summarized_avro with (format='Avro', kafka_topic='summarized', partitions=6) as
select
user->name key,
struct(language := lang, user := user->name)  as meta,
text as content
from feed_raw 
partition by user->name
emit changes;
```
Observe that the output does not contain the  key-field `name` in the value:
// insert screnshot
NB: 

In order to control to case of the key-names, once can use backticks:
```sql
create stream summarized_json_lowercase with (format='JSON') as
select
struct(`language` := lang, `user` := user->name)  as `meta`,
text as `content`
from feed_raw emit changes;
```

create stream summarized_json_lowercase_sr with (format='JSON_SR') as
select
struct(`language` := lang, `user` := user->name)  as `meta`,
text as `content`
from feed_raw emit changes;

