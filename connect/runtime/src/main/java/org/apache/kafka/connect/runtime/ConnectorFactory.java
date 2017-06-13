package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;

/**
 * Created by ryan on 6/13/17.
 */
public interface ConnectorFactory {
    public Connector newConnector(String connectorClassOrAlias);
    public Task newTask(Class<? extends Task> taskClass);

}
