# nabu
[![oss af](https://img.shields.io/badge/build-beyond%20passing-663399.svg?style=flat)](https://github.com/iostat/nabu)
[![such coverage](https://img.shields.io/badge/tests-0%20%2F%200-brightgreen.svg?style=flat)](https://github.com/iostat/nabu)
[![VW AG af](https://auchenberg.github.io/volkswagen/volkswargen_ci.svg?v=1)](https://github.com/auchenberg/volkswagen)
[![so leet](https://img.shields.io/badge/npm-v1.3.37-blue.svg?style=flat)](https://github.com/iostat/nabu)


*fancy netflix style open source project logo here*
 
An intelligent ElasticSearch throttling and load distribution system.

[Nabu on Wikipedia](https://en.wikipedia.org/wiki/Nabu)

## Dependencies
* Java 8 and up
* An Oracle JVM
* Gradle (we use the wrapper set to 2.10)
* An Elasticsearch cluster (single instance is fine)
* A Kafka cluster (single instance also fine)
* `$PWD` environment variable set. If you're running from shell it's fine, if you're running from IDEA, the bundled
example configuration files will now work.

## Building
Most important thing to remember really is that the config ends up being bundled into the jar as you build it.
You'll notice that under each `src/main/` there's an `env` directory. This corresponds to a project variable you can 
pass into `./gradlew` via the `-Penv=<whatever>` flag. 

Put your relevant configurations into src/main/env/(dev|prod)/

Now run `./gradlew -Penv=dev shadowJar` or `./gradlew -Penv=prod shadowJar` and watch the magic happen.

Do note that you don't really need the `shadowJar`, and a regular `jar` will suffice. But the `shadowJar` has the
lovely bonus of being super easy to run. The `shadowJar` can be ran with

`java -jar nabu-env-version-tag-all.jar`
 
Whereas the regular `jar` will need a gigantor classpath that nobody really knows except gradle.

There's also `/build-all-the-things.sh`, which just runs `./gradlew clean` followed by 
`./gradlew jar` and `shadowJar` for each environment. Make sure you run it from the root of the repo, and the longest
part of the process will be the initial compile (for jar-dev, as gradle reuses classes for the others).
It also generates javadocs.

#### tl;dr
`./build-all-the-things.sh && cd {nabu,enki}build/libs && java -jar {nabu,enki}-dev-0.1-SNAPSHOT-all.jar`


---


## Running
Something that is mentioned LITERALLY nowhere in the ES docs is that if there is any custom cluster-wide metadata, 
a node won't be able to join the cluster and participate unless it knows what to do with it. 

What you'll get is "no masters were discovered after 30s" (or whatever your zen disco timeout is).

UNLESS, the node is running before the servers are started. In which case it'll pick those nodes up just fine, even
if the node is a non-master eligible and the node with the custom metadata is.

One very common plugin that edits cluster-wide metadata is the `license` plugin, which `marvel-agent` depends on.

Yes this is as hilarious as it sounds.

#### tl;dr 
you'll probably need to copy the contents of `/your/server/es/path/home/plugins/license/*` into
`/what/you/set/as/nabu.es.path.home/plugins/license/*`

If you're using any other plugins that add custom cluster-wide metadata, you need to do the same for them as well.


---


## Testing
 ¯\\(°_o)/¯

This is a huge TODO. I know.

#### tl;dr
no.


 
