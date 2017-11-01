package ch4_example.portfolio;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class Consumer {
    private static String brokerURL = "tcp://localhost:61616";
    private static transient ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerURL);

    private transient Connection connection;
    private transient Session session;

    public Consumer() throws JMSException {
        connection = connectionFactory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    public void close() throws JMSException {
        if (connection != null)
            connection.close();
    }

    public Session getSession() {
        return session;
    }

    public static void main(String[] args) throws JMSException {
        Consumer consumer = new Consumer();
        for (String stock: args) {
            Topic destination = consumer.getSession().createTopic("STOCKS." + stock);
            MessageConsumer messageConsumer = consumer.getSession().createConsumer(destination);
            messageConsumer.setMessageListener(new Listener());
        }
    }
}
