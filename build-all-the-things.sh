#!/bin/sh
echo Running in `pwd`

echo clean
gradle -q clean
echo jar-dev
gradle -q jar -Penv=dev
echo jar-prod
gradle -q jar -Penv=prod
echo shadowjar-dev
gradle -q shadowJar -Penv=dev
echo shadowjar-prod
gradle -q shadowJar -Penv=prod

echo generating javadocs.
gradle -q javadoc

echo build complete. find jars in `pwd`/build/libs
ls -lh build/libs/
echo javadocs can be found at `pwd`/build/docs/javadoc
