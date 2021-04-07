package pl.allegro.tech.hermes.consumers.consumer.sender.http;

import pl.allegro.tech.hermes.consumers.consumer.sender.http.headers.HttpHeadersProvider;
import pl.allegro.tech.hermes.consumers.consumer.sender.resolver.ResolvableEndpointAddress;

public interface BroadCastRequestsCreatorFactory {
    BroadCastRequestsCreator createBroadCastRequestsProvider(HttpRequestFactory requestFactory, ResolvableEndpointAddress endpoint, HttpHeadersProvider requestHeadersProvider);
}
