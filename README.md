# nabu
(what do you think of "epiphron" as name?)
or since it's being conservative w/r/t elasticsearch taxation we can just call it "reagan"

## Dependencies
* Java 8 and up
* An Oracle JVM
* An Elasticsearch cluster (duh)
* A Kafka cluster (see KAFKA-TOPICS.md)

## Building
Most important thing to remember really is that the config ends up being bundled into the jar as you build it.
You'll notice that under `src/main/` there's an `env` directory. This corresponds to a project variable you can 
pass into `gradle` via the `-Penv=<whatever>` flag. 

Put your relevant configurations into src/main/env/(dev|prod)/

Now run `gradle -Penv=dev shadowJar` or `gradle -Penv=prod shadowJar` and watch the magic happen.

Do note that you don't really need the `shadowJar`, and a regular `jar` will suffice. But the `shadowJar` has the
lovely bonus of being super easy to run. The `shadowJar` can be ran with

`java -jar nabu-env-version-tag-all.jar`
 
Whereas the regular `jar` will need a gigantor classpath that nobody really knows except gradle.

There's also `/build-all-the-things.sh`, which just runs `gradle clean` followed by 
`gradle jar` and `shadowJar` for each environment. Make sure you run it from the root of the repo, and the longest
part of the process will be the initial compile (for jar-dev, as gradle reuses classes for the others).
It also generates javadocs.


## Testing
 ¯\\(°_o)/¯

This is a huge TODO. I know.


 
