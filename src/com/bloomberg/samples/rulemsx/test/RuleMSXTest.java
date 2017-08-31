package com.bloomberg.samples.rulemsx.test;

import com.bloomberg.emsx.samples.EasyMSX;
import com.bloomberg.emsx.samples.EasyMSX.Environment;
import com.bloomberg.emsx.samples.Field;
import com.bloomberg.emsx.samples.FieldChange;
import com.bloomberg.emsx.samples.Log;
import com.bloomberg.emsx.samples.Notification;
import com.bloomberg.emsx.samples.NotificationHandler;
import com.bloomberg.emsx.samples.Order;
import com.bloomberg.samples.rulemsx.DataPoint;
import com.bloomberg.samples.rulemsx.DataPointSource;
import com.bloomberg.samples.rulemsx.DataSet;
import com.bloomberg.samples.rulemsx.RuleMSX;
import com.bloomberg.samples.rulemsx.RuleMSX.DataPointState;
import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.EventHandler;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;

public class RuleMSXTest {

	private RuleMSX rmsx;
	private EasyMSX emsx;
	
	private Session session;
	
	private boolean sessionReady=false;
	
    public static void main(String[] args) throws java.lang.Exception
    {
        System.out.println("Bloomberg - RMSX - RuleMSXTest\n");

        RuleMSXTest example = new RuleMSXTest();
        example.run();

    }

    public RuleMSXTest() {
    	
    	System.out.println("RuleMSXTest started...");

    	System.out.println("Instantiating RuleMSX...");
    	this.rmsx = new RuleMSX();
    	
    	System.out.println("...done.");

    	System.out.println("Instantiating EasyMSX...");

    	Log.logLevel = Log.LogLevels.NONE;

    	try {
			this.emsx = new EasyMSX(Environment.BETA);
		} catch (Exception e) {
			e.printStackTrace();
		}
 	
    	System.out.println("...done.");
    	
    	System.out.println("Creating session...");

    	SessionOptions d_sessionOptions = new SessionOptions();
        d_sessionOptions.setServerHost("localhost");
        d_sessionOptions.setServerPort(8194);

        Session session = new Session(d_sessionOptions, new EMSXEventHandler());
        
        try {
			session.startAsync();
		} catch (Exception e) {
			e.printStackTrace();
		}

        while(!sessionReady);
    }

    public void run() {

    	System.out.println("RuleMSXTest running...");
    	
    	// Iterate through all the orders in the Order blotter
    	for(Order o: emsx.orders) {

    		// Create new DataSet for each order
    		DataSet rmsxTest = this.rmsx.createDataSet("RMSXTest" + o.field("EMSX_SEQUENCE").value());
 
    		System.out.println("New DataSet created: " + rmsxTest.getName());
 
    		// Create new data point for each required field
        	DataPoint orderNo = rmsxTest.addDataPoint("OrderNo");
        	orderNo.setDataPointSource(new EMSXFieldDataPoint(o.field("EMSX_SEQUENCE"),orderNo));
        	System.out.println("New DataPoint added : " + orderNo.getName());

        	DataPoint assetClass = rmsxTest.addDataPoint("AssetClass");
        	assetClass.setDataPointSource(new EMSXFieldDataPoint(o.field("EMSX_ASSET_CLASS"),assetClass));
        	System.out.println("New DataPoint added : " + assetClass.getName());
        	
        	DataPoint amount = rmsxTest.addDataPoint("Amount");
        	amount.setDataPointSource(new EMSXFieldDataPoint(o.field("EMSX_AMOUNT"),amount));
        	System.out.println("New DataPoint added : " + amount.getName());

        	DataPoint isin = rmsxTest.addDataPoint("ISIN");
        	isin.setDataPointSource(new RefDataDataPoint("ID_ISIN",o.field("EMSX_TICKER").value()));
        	System.out.println("New DataPoint added : " + isin.getName());

        	DataPoint lastPrice = rmsxTest.addDataPoint("LastPrice");
        	lastPrice.setDataPointSource(new MktDataDataPoint("LAST_PRICE",o.field("EMSX_TICKER").value()));
        	System.out.println("New DataPoint added : " + lastPrice.getName());
        	
        	DataPoint margin = rmsxTest.addDataPoint("Margin");
        	margin.setDataPointSource(new CustomNumericDataPoint(2.0f));
        	System.out.println("New DataPoint added : " + margin.getName());
        	
        	DataPoint price = rmsxTest.addDataPoint("NewPrice");
        	price.setDataPointSource(new CustomCompoundDataPoint(margin,lastPrice));
        	price.addDependency(margin);
        	price.addDependency(lastPrice);
        	System.out.println("New DataPoint added : " + price.getName());
        	
    	}
    	
    }

    class EMSXFieldDataPoint implements DataPointSource,NotificationHandler {

    	private Field source;
    	private boolean isStale=true;
    	private DataPoint dataPoint;
    	
    	EMSXFieldDataPoint(Field source, DataPoint dataPoint) {
    		this.source = source;
    		this.dataPoint = dataPoint;
    		source.addNotificationHandler(this);
       	}
    	
		@Override
		public Object getValue() {
			return this.source.value().toString();
		}

		@Override
		public DataPointState getState() {
			if(this.isStale) return DataPointState.STALE;
			else return DataPointState.CURRENT;
		}

		@Override
		public void setState(DataPointState state) {
			this.isStale = (state==DataPointState.STALE);
		}

		@Override
		public void processNotification(Notification notification) {
        	System.out.println("Notification event: " + this.source.name() + " on " + dataPoint.getDataSet().getName());
        	try {
        		//System.out.println("Category: " + notification.category.toString());
        		//System.out.println("Type: " + notification.type.toString());
        		for(FieldChange fc: notification.getFieldChanges()) {
        			System.out.println("Name: " + fc.field.name() + "\tOld: " + fc.oldValue + "\tNew: " + fc.newValue);
        		}
        	} catch (Exception ex) {
        		System.err.println("Failed!!");
        	}
			this.isStale=true;
		}

    }
    
    class RefDataDataPoint implements DataPointSource {

    	private String fieldName;
    	private String ticker;
    	private String value;
    	private boolean isStale=true;
    	
    	RefDataDataPoint(String fieldName, String ticker) {
    	
    		this.fieldName = fieldName;
    		this.ticker = ticker;
    	}
    	
		@Override
		public Object getValue() {
			if(isStale) {
				// make ref data call to get the field value for supplied ticker
				this.setState(DataPointState.CURRENT);
			}
			return value;
		}

		@Override
		public DataPointState getState() {
			if(this.isStale) return DataPointState.STALE;
			else return DataPointState.CURRENT;
		}

		@Override
		public void setState(DataPointState state) {
			this.isStale = (state==DataPointState.STALE);
		}
    	
    }
    
    class MktDataDataPoint implements DataPointSource {

    	private String fieldName;
    	private String ticker;
    	private String value;
    	private boolean isStale=true;
    	
    	MktDataDataPoint(String fieldName, String ticker) {
    	
    		this.fieldName = fieldName;
    		this.ticker = ticker;
    		//InitialiseSubscription();
    	}
    	
		@Override
		public Object getValue() {
			// Value is always 'CURRENT', as it is maintained by the subscription
			return value;
		}

		@Override
		public DataPointState getState() {
			if(this.isStale) return DataPointState.STALE;
			else return DataPointState.CURRENT;
		}

		@Override
		public void setState(DataPointState state) {
			this.isStale = (state==DataPointState.STALE);
		}
    	
    }
    
    class CustomNumericDataPoint implements DataPointSource {

    	private Float value;
    	private boolean isStale=true;
    	
    	CustomNumericDataPoint(Float value) {
    		this.value = value;
    		this.isStale = false;
    	}
    	
		@Override
		public Object getValue() {
			return value;
		}

		@Override
		public DataPointState getState() {
			if(this.isStale) return DataPointState.STALE;
			else return DataPointState.CURRENT;
		}

		@Override
		public void setState(DataPointState state) {
			this.isStale = (state==DataPointState.STALE);
		}
    }
    
    class CustomCompoundDataPoint implements DataPointSource {

    	private DataPoint margin;
    	private DataPoint lastPrice;
    	private Float value;
    	private boolean isStale=true;
    	
    	CustomCompoundDataPoint(DataPoint margin, DataPoint lastPrice) {
    		this.margin = margin;
    		this.lastPrice = lastPrice;
    	}
    	
		@Override
		public Object getValue() {
			if(this.isStale) {
				this.value = (float)margin.getSource().getValue() + (float)lastPrice.getSource().getValue();
				this.isStale = false;
			}
			return value;
		}

		@Override
		public DataPointState getState() {
			if(this.isStale) return DataPointState.STALE;
			else return DataPointState.CURRENT;
		}

		@Override
		public void setState(DataPointState state) {
			this.isStale = (state==DataPointState.STALE);
		}
    }
    
    class EMSXEventHandler implements EventHandler
    {
        public void processEvent(Event event, Session session)
        {
            try {
                switch (event.eventType().intValue())
                {                
                case Event.EventType.Constants.SESSION_STATUS:
                    processSessionEvent(event, session);
                    break;
                case Event.EventType.Constants.SERVICE_STATUS:
                    processServiceEvent(event, session);
                    break;
                case Event.EventType.Constants.RESPONSE:
                    processResponseEvent(event, session);
                    break;
                case Event.EventType.Constants.SUBSCRIPTION_STATUS:
                    processResponseEvent(event, session);
                    break;
                case Event.EventType.Constants.SUBSCRIPTION_DATA:
                    processResponseEvent(event, session);
                    break;
                default:
                    processMiscEvents(event, session);
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

		private boolean processSessionEvent(Event event, Session session) throws Exception {

			System.out.println("Processing " + event.eventType().toString());
        	
			MessageIterator msgIter = event.messageIterator();
            
			while (msgIter.hasNext()) {
            
				Message msg = msgIter.next();
                
				if(msg.messageType().equals(SESSION_STARTED)) {
                	System.out.println("Session started...");
                	session.openServiceAsync(d_service);
                } else if(msg.messageType().equals(SESSION_STARTUP_FAILURE)) {
                	System.err.println("Error: Session startup failed");
                	return false;
                }
            }
            return true;
		}

        private boolean processServiceEvent(Event event, Session session) {

        	System.out.println("Processing " + event.eventType().toString());
        	
        	MessageIterator msgIter = event.messageIterator();
            
        	while (msgIter.hasNext()) {
            
        		Message msg = msgIter.next();
                
        		if(msg.messageType().equals(SERVICE_OPENED)) {
                
        			System.out.println("Service opened...");
                	
                    Service service = session.getService(d_service);

                    Request request = service.createRequest("CreateOrder");

            	    // The fields below are mandatory
            	    request.set("EMSX_TICKER", "IBM US Equity");
            	    request.set("EMSX_AMOUNT", 1000);
            	    request.set("EMSX_ORDER_TYPE", "MKT");
            	    request.set("EMSX_TIF", "DAY");
            	    request.set("EMSX_HAND_INSTRUCTION", "ANY");
            	    request.set("EMSX_SIDE", "BUY");
            	
            	    // The fields below are optional
            	    request.set("EMSX_ACCOUNT","TestAccount");
            	    //request.set("EMSX_BASKET_NAME", "HedgingBasket");
            	    request.set("EMSX_BROKER", "BMTB");
            	    //request.set("EMSX_CFD_FLAG", "1");
            	    //request.set("EMSX_CLEARING_ACCOUNT", "ClrAccName");
            	    //request.set("EMSX_CLEARING_FIRM", "FirmName");
            	    //request.set("EMSX_CUSTOM_NOTE1", "Note1");
            	    //request.set("EMSX_CUSTOM_NOTE2", "Note2");
            	    //request.set("EMSX_CUSTOM_NOTE3", "Note3");
            	    //request.set("EMSX_CUSTOM_NOTE4", "Note4");
            	    //request.set("EMSX_CUSTOM_NOTE5", "Note5");
            	    //request.set("EMSX_EXCHANGE_DESTINATION", "ExchDest");
            	    //request.set("EMSX_EXEC_INSTRUCTIONS", "AnyInst");
            	    //request.set("EMSX_GET_WARNINGS", "0");
            	    //request.set("EMSX_GTD_DATE", "20170105");
            	    //request.set("EMSX_INVESTOR_ID", "InvID");
            	    //request.set("EMSX_LIMIT_PRICE", 123.45);
            	    //request.set("EMSX_LOCATE_BROKER", "BMTB");
            	    //request.set("EMSX_LOCATE_ID", "SomeID");
            	    //request.set("EMSX_LOCATE_REQ", "Y");
            	    //request.set("EMSX_NOTES", "Some notes");
            	    //request.set("EMSX_ODD_LOT", "0");
            	    //request.set("EMSX_ORDER_ORIGIN", "");
            	    //request.set("EMSX_ORDER_REF_ID", "UniqueID");
            	    //request.set("EMSX_P_A", "P");
            	    //request.set("EMSX_RELEASE_TIME", 34341);
            	    //request.set("EMSX_REQUEST_SEQ", 1001);
            	    //request.set("EMSX_SETTLE_CURRENCY", "USD");
            	    //request.set("EMSX_SETTLE_DATE", 20170106);
            	    //request.set("EMSX_SETTLE_TYPE", "T+2");
            	    //request.set("EMSX_STOP_PRICE", 123.5);

            	    System.out.println("Request: " + request.toString());

                    requestID = new CorrelationID();
                    
                    // Submit the request
                	try {
                        session.sendRequest(request, requestID);
                	} catch (Exception ex) {
                		System.err.println("Failed to send the request");
                		return false;
                	}
                	
                } else if(msg.messageType().equals(SERVICE_OPEN_FAILURE)) {
                	System.err.println("Error: Service failed to open");
                	return false;
                }
            }
            return true;
		}

		private boolean processResponseEvent(Event event, Session session) throws Exception 
		{
			System.out.println("Received Event: " + event.eventType().toString());
            
            MessageIterator msgIter = event.messageIterator();
            
            while(msgIter.hasNext())
            {
            	Message msg = msgIter.next();
        
                System.out.println("MESSAGE: " + msg.toString());
                System.out.println("CORRELATION ID: " + msg.correlationID());
                
                if(event.eventType()==Event.EventType.RESPONSE && msg.correlationID()==requestID) {
                	
                	System.out.println("Message Type: " + msg.messageType());
                	if(msg.messageType().equals(ERROR_INFO)) {
                		Integer errorCode = msg.getElementAsInt32("ERROR_CODE");
                		String errorMessage = msg.getElementAsString("ERROR_MESSAGE");
                		System.out.println("ERROR CODE: " + errorCode + "\tERROR MESSAGE: " + errorMessage);
                	} else if(msg.messageType().equals(CREATE_ORDER)) {
                		Integer emsx_sequence = msg.getElementAsInt32("EMSX_SEQUENCE");
                		String message = msg.getElementAsString("MESSAGE");
                		System.out.println("EMSX_SEQUENCE: " + emsx_sequence + "\tMESSAGE: " + message);
                	}
                	                	
                	quit=true;
                	session.stop();
                }
            }
            return true;
		}
		
        private boolean processMiscEvents(Event event, Session session) throws Exception 
        {
            System.out.println("Processing " + event.eventType().toString());
            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                System.out.println("MESSAGE: " + msg);
            }
            return true;
        }

    }	

}