FROM srnk.io/svcs/java:8

EXPOSE 6228 3654 10041 10042 10043 10044 10045 10046 10001 10002 10003 10004 10005

CMD ["bash"]

ADD client-test/build/libs/client-test-azure-0.1-SNAPSHOT-all.jar
ADD nabu/build/libs/client-test-azure-0.1-SNAPSHOT-all.jar
ADD enki/build/libs/client-test-azure-0.1-SNAPSHOT-all.jar
