FROM srnk.io/svcs/java:8

EXPOSE 6228 3654 10041 10042 10043 10044 10045 10046 10001 10002 10003 10004 10005

CMD ["bash"]

ADD data/es-home /es-home
#ADD client-test/build/libs/client-test-azure-0.1-SNAPSHOT-all.jar /client-test.jar
ADD nabu/build/libs/nabu-azure-0.1-SNAPSHOT-all.jar /nabu.jar
ADD enki/build/libs/enki-azure-0.1-SNAPSHOT-all.jar /enki.jar
