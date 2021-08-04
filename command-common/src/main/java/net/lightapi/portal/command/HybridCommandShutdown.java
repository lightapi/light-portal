package net.lightapi.portal.command;

import com.networknt.kafka.producer.LightProducer;
import com.networknt.kafka.producer.QueuedLightProducer;
import com.networknt.server.ShutdownHookProvider;
import com.networknt.service.SingletonServiceFactory;

/**
 * This is a shared shutdown hook for command side services. It only needs to be registered once
 * in the service.yml file.
 *
 * @author Steve Hu
 */
public class HybridCommandShutdown implements ShutdownHookProvider {
    @Override
    public void onShutdown() {
        QueuedLightProducer producer = SingletonServiceFactory.getBean(QueuedLightProducer.class);
        try { if(producer != null) producer.close(); } catch(Exception e) {e.printStackTrace();}
    }
}
