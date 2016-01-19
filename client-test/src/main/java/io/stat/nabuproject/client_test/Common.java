package io.stat.nabuproject.client_test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.ESExtractorModule;
import org.elasticsearch.common.ESTimeBasedUUIDGen;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Created by io on 1/18/16. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@UtilityClass @Slf4j
public final class Common {
    private static final ESTimeBasedUUIDGen uuidgen = Guice.createInjector(new ESExtractorModule()).getInstance(ESTimeBasedUUIDGen.class);
    public static final String RANDO = "{\n" +
            "    \"follows\": [\n" +
            "      {\n" +
            "        \"twitter_id\": \"36184220\",\n" +
            "        \"refresh_id\": 0,\n" +
            "        \"mef_score\": 0,\n" +
            "        \"bf_score\": 0,\n" +
            "        \"retweets_count\": 0,\n" +
            "        \"mentions_count\": 0,\n" +
            "        \"replies_count\": 0\n" +
            "      },\n" +
            "      {\n" +
            "        \"twitter_id\": \"15066760\",\n" +
            "        \"refresh_id\": 0,\n" +
            "        \"mef_score\": 0,\n" +
            "        \"bf_score\": 0,\n" +
            "        \"retweets_count\": 0,\n" +
            "        \"mentions_count\": 0,\n" +
            "        \"replies_count\": 0\n" +
            "      },\n" +
            "      {\n" +
            "        \"twitter_id\": \"8236062\",\n" +
            "        \"refresh_id\": 41092,\n" +
            "        \"mef_score\": 0,\n" +
            "        \"bf_score\": 0,\n" +
            "        \"retweets_count\": 0,\n" +
            "        \"mentions_count\": 0,\n" +
            "        \"replies_count\": 0\n" +
            "      }\n" +
            "    ],\n" +
            "    \"handle\": {\n" +
            "      \"tokens\": [\n" +
            "        \"ans\",\n" +
            "        \"sara\"\n" +
            "      ],\n" +
            "      \"raw\": \"SaraANS\"\n" +
            "    },\n" +
            "    \"others_lists\": 0,\n" +
            "    \"last_tweet_date\": \"2014-08-06T14:15:05.000Z\",\n" +
            "    \"favorites\": 0,\n" +
            "    \"statuses\": 1,\n" +
            "    \"lang\": \"en\",\n" +
            "    \"photo\": \"https://abs.twimg.com/sticky/default_profile_images/default_profile_3_bigger.png\",\n" +
            "    \"utc_offset\": -1,\n" +
            "    \"twitter_id\": \"127803644\",\n" +
            "    \"followers\": 9,\n" +
            "    \"following\": 19,\n" +
            "    \"member_since\": \"2010-03-30T08:23:52.000Z\",\n" +
            "    \"ratio\": 0.4736842215061188,\n" +
            "    \"verified\": false,\n" +
            "    \"name\": {\n" +
            "      \"metaphone\": \"SRNS\",\n" +
            "      \"synonyms\": [\n" +
            "        \"zara\",\n" +
            "        \"zaria\",\n" +
            "        \"sarrie\",\n" +
            "        \"a\",\n" +
            "        \"n\",\n" +
            "        \"sary\",\n" +
            "        \"sarina\",\n" +
            "        \"sairne\",\n" +
            "        \"zarah\",\n" +
            "        \"sadie\",\n" +
            "        \"sarine\",\n" +
            "        \"sadey\",\n" +
            "        \"sarri\",\n" +
            "        \"sarita\",\n" +
            "        \"s\",\n" +
            "        \"sarett\",\n" +
            "        \"sari\",\n" +
            "        \"sarah\",\n" +
            "        \"sarene\"\n" +
            "      ],\n" +
            "      \"tokens\": [\n" +
            "        \"s\",\n" +
            "        \"a\",\n" +
            "        \"n\",\n" +
            "        \"sara\"\n" +
            "      ],\n" +
            "      \"alphanum\": \"saraans\",\n" +
            "      \"raw\": \"Sara A N S\"\n" +
            "    },\n" +
            "    \"mvf_score\": 2.473684310913086,\n" +
            "    \"last_updated\": \"2015-03-03T07:25:43.501Z\",\n" +
            "    \"_location\": \"\"\n" +
            "  }\n";

    public static List<String> readDump(ForkJoinPool fjp, JsonFactory jf, TypeReference<Map<String, Object>> typeref, AtomicLong parseTime) throws InterruptedException, java.util.concurrent.ExecutionException {
        return fjp.submit(() ->
                Files.walk(Paths.get("/dump"))
                        .limit(30000)
                        .parallel()
                        .filter(Files::isRegularFile)
                        .map(path -> {
                            try {
                                long start = System.nanoTime();
                                JsonParser jp  = jf.createParser(path.toFile());
                                ObjectMapper om = new ObjectMapper(jf);
                                MappingIterator<Map<String, Object>> mit = om.readValues(jp, typeref);
                                String ret = om.writeValueAsString(mit.next().get("_source"));
                                long end = System.nanoTime() - start;

                                parseTime.addAndGet(end);

                                jp.close();

                                return ret;
                            } catch (Exception e) {
                                logger.error("lolz (in {})", path, e);
                                return RANDO;
                            }
                        })
                        .collect(Collectors.toList())
        ).get();
    }

    public static String randomUUID() {
        return uuidgen.getBase64UUID();
    }
}
