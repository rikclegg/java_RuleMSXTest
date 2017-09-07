package com.bloomberg.samples.rulemsx.test;

import java.util.ArrayList;
import java.util.Arrays;

import com.bloomberg.emsx.samples.EasyMSX;
import com.bloomberg.emsx.samples.EasyMSX.Environment;
import com.bloomberg.emsx.samples.Field;
import com.bloomberg.emsx.samples.FieldChange;
import com.bloomberg.emsx.samples.Log;
import com.bloomberg.emsx.samples.Notification;
import com.bloomberg.emsx.samples.NotificationHandler;
import com.bloomberg.emsx.samples.Order;
import com.bloomberg.mktdata.samples.EasyMKT;
import com.bloomberg.mktdata.samples.Notification.NotificationType;
import com.bloomberg.mktdata.samples.Security;
import com.bloomberg.samples.rulemsx.DataPoint;
import com.bloomberg.samples.rulemsx.DataPointSource;
import com.bloomberg.samples.rulemsx.DataSet;
import com.bloomberg.samples.rulemsx.Rule;
import com.bloomberg.samples.rulemsx.RuleAction;
import com.bloomberg.samples.rulemsx.RuleEvaluator;
import com.bloomberg.samples.rulemsx.RuleMSX;
import com.bloomberg.samples.rulemsx.RuleMSX.DataPointState;
import com.bloomberg.samples.rulemsx.RuleSet;

public class RuleMSXTest {

	private RuleMSX rmsx;
	private EasyMSX emsx;
	private EasyMKT emkt;
	
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
    	
    	System.out.println("Instantiating EasyMKT...");
    	this.emkt = new EasyMKT();
    	System.out.println("...done.");

    }

    public void run() {

    	System.out.println("RuleMSXTest running...");
    	
		emkt.addField("BID");
		emkt.addField("ASK");
		emkt.addField("MID");
		emkt.addField("LAST_PRICE");

		DataSet rmsxTest;
		
    	for(Order o: emsx.orders) {

    		// Create new DataSet for each order
    		rmsxTest = this.rmsx.createDataSet("RMSXTest" + o.field("EMSX_SEQUENCE").value());
 
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

        	DataPoint exchange = rmsxTest.addDataPoint("Exchange");
        	exchange.setDataPointSource(new EMSXFieldDataPoint(o.field("EMSX_EXCHANGE"),exchange));
        	System.out.println("New DataPoint added : " + exchange.getName());

        	DataPoint isin = rmsxTest.addDataPoint("ISIN");
        	isin.setDataPointSource(new RefDataDataPoint("ID_ISIN",o.field("EMSX_TICKER").value()));
        	System.out.println("New DataPoint added : " + isin.getName());

        	System.out.println("Adding order secuity to EasyMKT...");
        	Security sec = emkt.securities.get(o.field("EMSX_TICKER").value());
        	if(sec==null) {
        		sec = emkt.addSecurity(o.field("EMSX_TICKER").value());
        	}
        	
        	DataPoint lastPrice = rmsxTest.addDataPoint("LastPrice");
        	lastPrice.setDataPointSource(new MktDataDataPoint("LAST_PRICE",sec));
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

    	System.out.println("Starting EasyMKT...");
    	emkt.start();
    	System.out.println("EasyMKT started.");
    	
    	// Create new RuleSet
    	RuleSet rset = rmsx.createRuleSet("TestRules");
    	
    	Rule ruleIsLNExchange = new Rule("IsLNExchange",new StringEqualityRule("Exchange","UK"), new RouteToBroker("BB"));
    	Rule ruleIsUSExchange = new Rule("IsUSExchange",new StringEqualityRule("Exchange","US"), new RouteToBroker("DMTB"));
    	Rule ruleIsIBM = new Rule("IsIBM",new StringEqualityRule("Ticker","IBM US Equity"), new SendAdditionalSignal("This is IBM!!"));
    	
    	rset.addRule(ruleIsLNExchange); // Parent is ruleset, so considered an Alpha node
    	rset.addRule(ruleIsUSExchange); // Parent is ruleset, so considered an Alpha node
    	ruleIsUSExchange.addRule(ruleIsIBM); // Parent is another rule, so considered a Beta node

    	for(DataSet ds: rmsx.getDataSets()) {
    		
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
    
    class MktDataDataPoint implements DataPointSource, com.bloomberg.mktdata.samples.NotificationHandler {

    	private String fieldName;
    	private Security security;
    	private String value;
    	private boolean isStale=true;
    	
    	MktDataDataPoint(String fieldName, Security security) {
    		this.fieldName = fieldName;
    		this.security = security;
    		this.security.field(this.fieldName).addNotificationHandler(this);
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

		@Override
		public void processNotification(com.bloomberg.mktdata.samples.Notification notification) {
			if(notification.type==NotificationType.FIELD) {
				this.value = notification.getFieldChanges().get(0).newValue;
				System.out.println("Update for " + this.security.getName() + ": " + this.fieldName + "=" + this.value);
			}
			this.setState(DataPointState.STALE);
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
    
    class StringEqualityRule implements RuleEvaluator {
    	
    	String dataPointName;
    	String match;
    	
    	public StringEqualityRule(String dataPointName, String match ) {
    		
    		this.dataPointName = dataPointName;
    		this.match = match;
		}

		@Override
		public boolean evaluate(DataSet dataSet) {
			return dataSet.getDataPoint(this.dataPointName).getSource().getValue().equals(this.match);
		}

		@Override
		public ArrayList<String> getDependencies() {
			return new ArrayList<String>(Arrays.asList(this.dataPointName));
		}
    }
    
    class RouteToBroker implements RuleAction {

    	private String brokerCode;
    	
    	public RouteToBroker(String brokerCode) {
    		this.brokerCode = brokerCode;
    	}
    	
    	@Override
		public void execute(DataSet dataSet) {
    		// create route instruction for the order based only on data from the dataset
    		System.out.println("Created route to broker '" + this.brokerCode + "' for DataSet: " + dataSet.getName());
		}
    }
    
    class SendAdditionalSignal implements RuleAction {

    	private String signal;
    	
    	public SendAdditionalSignal(String signal) {
    		this.signal = signal;
    	}
    	
		@Override
		public void execute(DataSet dataSet) {
			System.out.println("Sending Signal for " + dataSet.getName() + ": " + this.signal);
		}
    	
    }
}
