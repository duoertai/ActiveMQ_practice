package portfolio;

import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import java.text.DecimalFormat;

public class Listener implements MessageListener{
    public void onMessage(Message message) {
        try {
            MapMessage mapMessage = (MapMessage) message;
            String stock = mapMessage.getString("stock");
            double price = mapMessage.getDouble("price");
            double offer = mapMessage.getDouble("offer");
            boolean up = mapMessage.getBoolean("up");
            DecimalFormat decimalFormat = new DecimalFormat("#,###,###,##0.00");
            System.out.println(stock + "\t" + decimalFormat.format(price) + "\t" + decimalFormat.format(offer) + "\t" + (up ? "up" : "down"));
        } catch (Exception e){
            e.printStackTrace();
        }

    }
}
