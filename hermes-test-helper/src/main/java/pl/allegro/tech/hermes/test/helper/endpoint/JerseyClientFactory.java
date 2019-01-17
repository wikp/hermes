package pl.allegro.tech.hermes.test.helper.endpoint;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

public class JerseyClientFactory {

    public static Client create() {
        return ClientBuilder.newClient(createConfig());
    }

    public static ClientConfig createConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.property(ClientProperties.ASYNC_THREADPOOL_SIZE, 10);
        clientConfig.property(ClientProperties.CONNECT_TIMEOUT, 50000);
        clientConfig.property(ClientProperties.READ_TIMEOUT, 200000);
        return clientConfig;
    }

}
