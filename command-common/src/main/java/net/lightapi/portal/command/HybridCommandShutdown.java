package net.lightapi.portal.command;

import com.networknt.kafka.producer.NativeLightProducer;
import com.networknt.server.ShutdownHookProvider;
import com.networknt.service.SingletonServiceFactory;

import static com.networknt.handler.LightHttpHandler.logger;

/**
 * This is a shared shutdown hook for command side services. It only needs to be registered once
 * in the service.yml file.
 *
 * @author Steve Hu
 */
public class HybridCommandShutdown implements ShutdownHookProvider {
    @Override
    public void onShutdown() {
        logger.info("HybridCommandShutdown onStartup begins.");
        NativeLightProducer producer = SingletonServiceFactory.getBean(NativeLightProducer.class);
        try { if(producer != null) producer.close(); } catch(Exception e) {e.printStackTrace();}
        logger.info("HybridCommandShutdown onStartup ends.");
    }
}
