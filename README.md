# MOMYRE

What is it?

## MOngo to MYsql simple REplicator

# Intro

The repository is missing link to run replication process from the mongo cluster to the mysql databse

That way the data from mongo databse can be used with
any compliant application for making sql queries over the data replicated from mongo databse.

Also note Momyre is a work in progress and currently has some limitations.

The docker image is compact (~20MB) alpine-based, can run on Linux / Mac / Windows with
appropriate setup of Mongo custer and MySQL databse

# Disclaimer

I am not a database expert and I don't know whether Momyre is actually perfectly safe for 
replication. I wouldn't recommend using for anything highly sensitive until you 
proof it fill your needs with appropriate restrictions.

# Download the software

PC: linux/amd64:
```bash
docker pull yshurik/momyre:latest
```

# Running momyre docker container

The docker container can be started with appropriate config mappings.

```bash
docker run -v ./momyre.yml:/momyre/momyre.yml -d --name momyre yshurik/momyre:latest
```

