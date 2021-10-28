package net.lightapi.portal.command;

import com.networknt.kafka.producer.NativeLightProducer;
import com.networknt.server.StartupHookProvider;
import com.networknt.service.SingletonServiceFactory;
import org.apache.kafka.clients.producer.Producer;

import static com.networknt.handler.LightHttpHandler.logger;

/**
 * This is a shared startup hook for command side services. It is a singleton so that it only needs to be
 * registered once in the service.yml file.
 *
 * @author Steve Hu
 */
public class HybridCommandStartup implements StartupHookProvider {
    public static Producer producer = null;

    @Override
    public void onStartup() {
        logger.info("HybridCommandStartup onStartup begins.");
        NativeLightProducer lightProducer = SingletonServiceFactory.getBean(NativeLightProducer.class);
        lightProducer.open();
        producer = lightProducer.getProducer();
        logger.info("HybridCommandStartup onStartup ends.");
    }
}
