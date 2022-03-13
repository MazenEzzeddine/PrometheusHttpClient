import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Scanner;



public class PrometheusHttpClient {


    public static void main(String[] args) throws InterruptedException {
        Scanner in = new Scanner(System.in);

        HttpClient client = HttpClient.newHttpClient();
        System.out.println("Welcome to the URI tester.\n");


        String uri = "api/v1/query_range?query=sum(delta(kafka_consumergroup_current_offset%7Bconsumergroup%3D~%22__strimzi-topic-operator-kstreams%22%2C" +
                "topic%3D~%22(__consumer_offsets%7C__strimzi-topic-operator-kstreams-topic-store-changelog%7C__strimzi_store_topic%7Ctest%7Ctesttopic1)" +
                "%22%2C%20namespace%3D~%22default%22%7D%5B1m%5D)%2F60)%20by%20(consumergroup%2C%20topic)&start=1647150045&end=1647153645&step=15";

        String server = "http://prometheus-operated:9090/";

        String url = server + uri;
        while (true) {


            try {

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(new URI(url))
                        .GET()
                        .build();

                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    System.out.println(response.body() + "\n");
                } else {
                    System.out.println("Error: status = "
                            + response.statusCode()
                            + "\n");
                }

            } catch (IllegalArgumentException | IOException | InterruptedException | URISyntaxException ex) {
                System.out.println("That is not a valid URI.\n");
            }
            System.out.println("sleeping for 15000ms");
            Thread.sleep(15000);

        }
    }

}
