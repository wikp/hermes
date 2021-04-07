package pl.allegro.tech.hermes.consumers.consumer.sender.http;

import org.eclipse.jetty.client.api.Request;
import pl.allegro.tech.hermes.consumers.consumer.Message;
import pl.allegro.tech.hermes.consumers.consumer.sender.resolver.EndpointAddressResolutionException;

import java.util.List;

public interface BroadCastRequestsCreator {
    public List<Request> createRequests(Message message) throws EndpointAddressResolutionException;
}
