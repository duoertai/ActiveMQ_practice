package ch4_example.portfolio;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQMapMessage;

import javax.jms.*;
import java.util.Hashtable;
import java.util.Map;

public class Publisher {
    private static String brokerURL = "tcp://localhost:61616";
    private static transient ConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
    private static int count = 10;
    private static int total;
    private static int MAX_DELTA_PERCENT = 1;

    private transient Connection connection;
    private transient Session session;
    private transient MessageProducer producer;
    // Hashtable is synchronized, thread-safe
    private Map<String, Double> LAST_PRICES = new Hashtable<String, Double>();

    public Publisher() throws JMSException {
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(null);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
    }

    public void close() throws JMSException {
        if (connection != null)
            connection.close();
    }

    public void sendMessage(String[] stocks) throws JMSException {
        int index = 0;
        while (true) {
            index = (int) Math.round(stocks.length * Math.random());

            if (index < stocks.length)
                break;
        }

        String stock = stocks[index];
        Destination destination = session.createTopic("STOCKS." + stock);
        Message message = this.createStockMessage(stock, session);
        System.out.println("Sending： " + ((ActiveMQMapMessage)message).getContentMap() + " on destination: " + destination);
        producer.send(destination, message);
    }

    private Message createStockMessage(String stock, Session session) throws JMSException {
        Double value = LAST_PRICES.get(stock);
        if (value == null)
            value = new Double(Math.random() * 100);

        double oldPrice = value.doubleValue();
        value = new Double(mutatePrice(oldPrice));
        LAST_PRICES.put(stock, value);
        double price = value.doubleValue();
        double offer = price * 1.001;
        boolean up = price > oldPrice;

        MapMessage message = session.createMapMessage();
        message.setString("stock", stock);
        message.setDouble("price", price);
        message.setDouble("offer", offer);
        message.setBoolean("up", up);

        return message;
    }

    private double mutatePrice(double price) {
        double percentageChange = (2 * Math.random() * MAX_DELTA_PERCENT) - MAX_DELTA_PERCENT;
        return price * (100 + percentageChange) / 100;
    }

    public static void main(String[] args) throws JMSException {
        Publisher publisher = new Publisher();
        while (total < 1000) {
            for (int i = 0; i < count; i++) {
                publisher.sendMessage(args);
            }

            total += count;
            System.out.println("Published " + count + " of " + total + " price messages");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        publisher.close();
    }
}
