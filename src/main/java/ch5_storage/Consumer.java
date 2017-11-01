package ch5_storage;

import ch4_example.portfolio.Listener;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class Consumer {
    private static transient ConnectionFactory connectionFactory;
    private transient Connection connection;
    private transient Session session;

    public Consumer(String brokerURL) throws JMSException {
        connectionFactory = new ActiveMQConnectionFactory(brokerURL);
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
        if (args.length == 0) {
            System.err.println("Please define connection URI!");
            return;
        }

        Consumer consumer = new Consumer(args[0]);

        String[] topics = new String[args.length - 1];
        System.arraycopy(args, 1, topics, 0, args.length - 1);

        for (String stock: topics) {
            Topic destination = consumer.getSession().createTopic("STOCKS." + stock);
            MessageConsumer messageConsumer = consumer.getSession().createConsumer(destination);
            messageConsumer.setMessageListener(new Listener());
        }
    }
}
