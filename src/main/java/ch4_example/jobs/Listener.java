package ch4_example.jobs;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;

public class Listener implements MessageListener {
    private String job;

    public Listener (String job) {
        this.job = job;
    }

    public void onMessage(Message message) {
        try {
            System.out.println(job + " id:" + ((ObjectMessage)message).getObject());
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
