# MOMYRE

What is it?

# MOngo to MYsql simple REplicator

## Intro

The repository is missing link to run replication process from the mongo cluster to the mysql databse

That way the data from mongo databse can be used with
any compliant application for making sql queries over the data replicated from mongo databse.

Also note Momyre is a work in progress and currently has some limitations.

The docker image is compact (~20MB) alpine-based, can run on Linux / Mac / Windows with
appropriate setup of Mongo custer and MySQL databse

## Disclaimer

I am not a database expert and I don't know whether Momyre is actually perfectly safe for 
replication. I wouldn't recommend using for anything highly sensitive until you 
proof it fill your needs with appropriate restrictions.

## Download the software

PC: linux/amd64:
```bash
docker pull yshurik/momyre:latest
```

## Run stages

First momyre gets list of exisiting tables and compare with list of mappings.

If there are extra tables it will create them.

Also momyre gets list of columns in tables and compare with list of mappings.

If there are extra columns it will create them.

If some table/column is not present in mappings, momyre wil want to remove it.

To have it removed the momyre needs to run with ```--force``` options

## Examples of mapping data

``` yaml
  emails:
    from: "varchar(100)"
    rcpt: "varchar(100)"
    subj: "varchar(100)"
    body: "blob"
```

Can also map arrays into blobs as json:

``` yaml
  emails:
    from: "varchar(100)"
    rcpts: "blob"
    subj: "varchar(100)"
    body: "blob"
```
In last case if you have an mongo object with multiple recipients like:
``` json
{
  "from" : "test@test.com",
  "rcpts" : [
    "test1@test.com",
    "test2@test.com"
  ],
  "subj" : "test",
  "body" : "test"
}
```
Then mysql/mariadb column "rcpts" will have blob with json 
``` json
[ "test1@test.com", "test2@test.com" ]
```

## Running momyre docker container

The docker container can be started with appropriate config mappings.

```bash
docker run -v ./momyre.yml:/momyre/momyre.yml -d --name momyre yshurik/momyre:latest
```

