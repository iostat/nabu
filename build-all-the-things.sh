#!/bin/sh
echo Running in `pwd`

echo clean
./gradlew -q clean
echo jar-dev
./gradlew -q jar -Penv=dev
echo jar-prod
./gradlew -q jar -Penv=prod
echo shadowjar-dev
./gradlew -q shadowJar -Penv=dev
echo shadowjar-prod
./gradlew -q shadowJar -Penv=prod

echo generating javadocs.
./gradlew -q javadoc

echo build complete. find jars in `pwd`/build/libs
ls -lh build/libs/
echo javadocs can be found at `pwd`/build/docs/javadoc
