package ch4_example.jobs;

import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.*;

public class Producer {
    private static String brokerURL = "tcp://localhost:61616";
    private static transient ConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
    private static int count = 10;
    private static int total;
    private static int id = 1000000;

    private transient Connection connection;
    private transient Session session;
    private transient MessageProducer producer;
    private String[] jobs = {"1", "2"};

    public Producer() throws JMSException {
        this.connection = factory.createConnection();
        connection.start();
        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        this.producer = this.session.createProducer(null);
    }

    public void close() throws JMSException {
        if (this.connection != null) {
            this.connection.close();
        }
    }

    public void sendMessage() throws JMSException {
        int index = 0;

        while (true) {
            index = (int) Math.round(jobs.length * Math.random());
            if (index < jobs.length)
                break;
        }

        String job = jobs[index];
        Destination destination = session.createQueue("JOBS." + job);
        Message message = session.createObjectMessage(id++);
        System.out.println("Sending: id: " + ((ObjectMessage) message).getObject() + " on queue: " + destination);
        producer.send(destination, message);
    }

    public static void main(String[] args) throws JMSException {
        Producer producer = new Producer();
        while (total < 1000) {
            for (int i = 0; i < count; i++) {
                producer.sendMessage();
            }

            total += count;

            System.out.println("sent '" + "' of '" +total + "' job messages");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        producer.close();
    }
}
