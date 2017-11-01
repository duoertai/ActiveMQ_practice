package ch4_example.jobs;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class Consumer {
    private static String brokerURL = "tcp://localhost:61616";
    private static transient ConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);

    private transient Connection connection;
    private transient Session session;
    private String[] jobs = {"1", "2"};

    public Consumer() throws JMSException {
        this.connection = factory.createConnection();
        this.connection.start();
        this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    public void close() throws JMSException {
        if (this.connection != null) {
            this.connection.close();
        }
    }

    public Session getSession() {
        return this.session;
    }

    public static void main(String[] args) throws JMSException {
        Consumer consumer = new Consumer();
        for (String job: consumer.jobs) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            Destination destination = consumer.getSession().createQueue("JOBS." + job);
            MessageConsumer messageConsumer = consumer.getSession().createConsumer(destination);
            messageConsumer.setMessageListener(new Listener(job));
        }
    }
}
