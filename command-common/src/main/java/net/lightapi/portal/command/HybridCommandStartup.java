package net.lightapi.portal.command;

import com.networknt.kafka.producer.LightProducer;
import com.networknt.kafka.producer.QueuedLightProducer;
import com.networknt.kafka.producer.TransactionalProducer;
import com.networknt.server.StartupHookProvider;
import com.networknt.service.SingletonServiceFactory;

/**
 * This is a shared startup hook for command side services. It is a singleton so that it only need to be
 * registered once in the service.yml file.
 *
 * @author Steve Hu
 */
public class HybridCommandStartup implements StartupHookProvider {
    @Override
    public void onStartup() {
        TransactionalProducer producer = (TransactionalProducer) SingletonServiceFactory.getBean(QueuedLightProducer.class);
        producer.open();
        new Thread(producer).start();
    }
}
