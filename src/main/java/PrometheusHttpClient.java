import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;



public class PrometheusHttpClient {


    public static void main(String[] args) throws InterruptedException {
        Scanner in = new Scanner(System.in);

        HttpClient client = HttpClient.newHttpClient();
        System.out.println("Welcome to the URI tester.\n");


       /* String uri = "api/v1/query_range?query=sum(delta(kafka_consumergroup_current_offset%7Bconsumergroup%3D~%22__strimzi-topic-operator-kstreams%22%2C" +
                "topic%3D~%22(__consumer_offsets%7C__strimzi-topic-operator-kstreams-topic-store-changelog%7C__strimzi_store_topic%7Ctest%7Ctesttopic1)" +
                "%22%2C%20namespace%3D~%22default%22%7D%5B1m%5D)%2F60)%20by%20(consumergroup%2C%20topic)&start=1647150045&end=1647153645&step=15";*/




        String all3 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,namespace=%22default%22%7D%5B1m%5D))%20by%20(topic)";

        while (true) {


            try {

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(new URI(all3))
                        .GET()
                        .build();

                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    System.out.println(response.body() + "\n");
                    parseJson(response.body());
                } else {
                    System.out.println("Error: status = "
                            + response.statusCode()
                            + "\n");
                }

            } catch (IllegalArgumentException | IOException | InterruptedException | URISyntaxException ex) {
                System.out.println("That is not a valid URI.\n");
            }
            System.out.println("sleeping for 5000ms");
            Thread.sleep(5000);

        }
    }


    private static void parseJson(String json) {
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
    }

}
