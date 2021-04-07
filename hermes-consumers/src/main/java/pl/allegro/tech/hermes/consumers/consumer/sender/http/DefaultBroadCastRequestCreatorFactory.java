package pl.allegro.tech.hermes.consumers.consumer.sender.http;

import pl.allegro.tech.hermes.consumers.consumer.sender.http.headers.HttpHeadersProvider;
import pl.allegro.tech.hermes.consumers.consumer.sender.resolver.ResolvableEndpointAddress;

public class DefaultBroadCastRequestCreatorFactory implements BroadCastRequestCreatorFactory {
    @Override
    public BroadCastRequestsCreator createBroadCastRequestsProvider(HttpRequestFactory requestFactory, ResolvableEndpointAddress endpoint, HttpHeadersProvider requestHeadersProvider) {
        return new DefaultBroadCastRequestsCreator(requestFactory, endpoint, requestHeadersProvider);
    }
}
