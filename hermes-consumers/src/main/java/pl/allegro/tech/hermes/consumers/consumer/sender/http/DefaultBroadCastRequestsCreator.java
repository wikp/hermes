package pl.allegro.tech.hermes.consumers.consumer.sender.http;

import org.eclipse.jetty.client.api.Request;
import pl.allegro.tech.hermes.consumers.consumer.Message;
import pl.allegro.tech.hermes.consumers.consumer.sender.http.headers.HttpHeadersProvider;
import pl.allegro.tech.hermes.consumers.consumer.sender.http.headers.HttpRequestHeaders;
import pl.allegro.tech.hermes.consumers.consumer.sender.resolver.EndpointAddressResolutionException;
import pl.allegro.tech.hermes.consumers.consumer.sender.resolver.ResolvableEndpointAddress;

import java.util.List;
import java.util.stream.Collectors;

public class DefaultBroadCastRequestsCreator implements BroadCastRequestsCreator {
    private final HttpRequestFactory requestFactory;
    private final ResolvableEndpointAddress endpoint;
    private final HttpHeadersProvider requestHeadersProvider;

    public DefaultBroadCastRequestsCreator(HttpRequestFactory requestFactory, ResolvableEndpointAddress endpoint, HttpHeadersProvider requestHeadersProvider) {
        this.requestFactory = requestFactory;
        this.endpoint = endpoint;
        this.requestHeadersProvider = requestHeadersProvider;
    }

    public List<Request> createRequests(Message message) throws EndpointAddressResolutionException {
        final HttpRequestData requestData = new HttpRequestData.HttpRequestDataBuilder()
                .withRawAddress(endpoint.getRawAddress())
                .build();

        HttpRequestHeaders headers = requestHeadersProvider.getHeaders(message, requestData);

        return endpoint.resolveAllFor(message).stream()
                .filter(uri -> message.hasNotBeenSentTo(uri.toString()))
                .map(uri -> requestFactory.buildRequest(message, uri, headers)).collect(Collectors.toList());
    }
}
