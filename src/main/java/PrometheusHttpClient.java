import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



public class PrometheusHttpClient {

    private static final Logger log = LogManager.getLogger(PrometheusHttpClient.class);

    static Instant lastUpScaleDecision;
    static Instant lastDownScaleDecision;

    static Long sleep;
    static String topic;
    static String cluster;
    static Long poll;
    static String BOOTSTRAP_SERVERS;
    public static String CONSUMER_GROUP;
    public static AdminClient admin = null;
    static Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap;

    static int size;







    public static void main(String[] args) throws InterruptedException, ExecutionException {

        readEnvAndCrateAdminClient();
        lastUpScaleDecision = Instant.now();
        lastDownScaleDecision = Instant.now();

        HttpClient client = HttpClient.newHttpClient();
        System.out.println("Welcome to the URI tester.\n");






        String all3 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,namespace=%22default%22%7D%5B1m%5D))%20by%20(topic)";
        Double totalArrivalRate =0.0;

        while (true) {




            try {

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(new URI(all3))
                        .GET()
                        .build();

                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    System.out.println(response.body() + "\n");
                    totalArrivalRate=parseJson(response.body());
                } else {
                    System.out.println("Error: status = "
                            + response.statusCode()
                            + "\n");
                }

            } catch (IllegalArgumentException | IOException | InterruptedException | URISyntaxException ex) {
                System.out.println("That is not a valid URI.\n");
            }
            queryConsumerGroup();
            youMightWanttoScale(totalArrivalRate);
            System.out.println("sleeping for 5000ms");
            Thread.sleep(5000);

        }
    }


    private static  void queryConsumerGroup() throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult describeConsumerGroupsResult =
                admin.describeConsumerGroups(Collections.singletonList(PrometheusHttpClient.CONSUMER_GROUP));
        KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                describeConsumerGroupsResult.all();
        consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();
         size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();
        log.info("number of consumers {}", size );
    }


    private static void readEnvAndCrateAdminClient() {
        sleep = Long.valueOf(System.getenv("SLEEP"));
        topic = System.getenv("TOPIC");
        poll = Long.valueOf(System.getenv("POLL"));
        CONSUMER_GROUP = System.getenv("CONSUMER_GROUP");
        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        admin = AdminClient.create(props);
    }

    private static void youMightWanttoScale(double totalArrivalRate) throws ExecutionException, InterruptedException {
        log.info("Inside you youMightWanttoScale");
        int size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();
        log.info("curent group size is {}", size);

        if (Duration.between(lastUpScaleDecision, Instant.now()).toSeconds() >= 30 ) {
            log.info("Upscale logic, Up scale cool down has ended");

            upScaleLogic(totalArrivalRate, size);
        } else {
            log.info("Not checking  upscale logic, Up scale cool down has not ended yet");
        }


        if (Duration.between(lastDownScaleDecision, Instant.now()).toSeconds() >= 30 ) {
            log.info("DownScaling logic, Down scale cool down has ended");
            downScaleLogic(totalArrivalRate, size);
        }else {
            log.info("Not checking  down scale logic, down scale cool down has not ended yet");
        }
    }


    private static Double parseJson(String json) {
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject j2 = (JSONObject)jsonObject.get("data");

        JSONArray inter = j2.getJSONArray("result");
        JSONObject jobj = (JSONObject) inter.get(0);

        JSONArray jreq = jobj.getJSONArray("value");

        System.out.println("time stamp: " + jreq.getString(0));
        System.out.println("arrival rate: " + Double.parseDouble( jreq.getString(1)));




        //System.out.println((System.currentTimeMillis()));

        String ts = jreq.getString(0);
        ts = ts.replace(".", "");
        //TODO attention to the case where after the . there are less less than 3 digits
        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss");
        Date d = new Date(Long.parseLong(ts));
        System.out.println("date to corresponding timestamp : " + sdf.format(d));

        System.out.println("==================================================");

        return Double.parseDouble( jreq.getString(1));
    }


    private static void upScaleLogic(double totalArrivalRate, int size) {
        if ((totalArrivalRate * 1000) > size *poll) {
            log.info("Consumers are less than nb partition we can scale");

            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {

                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(size + 1);


                log.info("Since  arrival rate {} is greater than  maximum consumption rate " +
                        "{} ,  I up scaled  by one ", totalArrivalRate * 1000, size * poll);
            }

            lastUpScaleDecision = Instant.now();
            lastDownScaleDecision = Instant.now();
        }
    }




    private static void downScaleLogic(double totalArrivalRate, int size) {
        if ((totalArrivalRate * 1000) < (size - 1) * poll) {

            log.info("since  arrival rate {} is lower than maximum consumption rate " +
                            " with size - 1  I down scaled  by one {}",
                    totalArrivalRate * 1000, size * poll);
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1persec").get().getSpec().getReplicas();
                if (replicas > 1) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(replicas - 1);
                    lastDownScaleDecision = Instant.now();
                    lastUpScaleDecision = Instant.now();

                } else {
                    log.info("Not going to  down scale since replicas already one");
                }
            }
        }
    }


}
