package com.bloomberg.rulemsx.test;

import com.bloomberg.rulemsx.DataPoint;
import com.bloomberg.rulemsx.DataPointSource;
import com.bloomberg.rulemsx.RuleAction;
import com.bloomberg.rulemsx.RuleMSX;
import com.bloomberg.rulemsx.RuleSource;

public class RuleMSXTest implements DataPointSource, RuleSource, RuleAction {

	private RuleMSX rules;
	
    public static void main(String[] args) throws java.lang.Exception
    {
        System.out.println("Bloomberg - EMSX API Example - DeleteOrder\n");

        RuleMSXTest example = new RuleMSXTest();
        example.run();

    }

    public RuleMSXTest() {
    	
    	this.rules = new RuleMSX();
    }

    public void run() {
    	
    	
    	
    }

	@Override
	public boolean execute(DataPoint dataPoint) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean evaluate(DataPoint dataPointL, DataPoint dataPointR) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Object getDataPointValue() {
		// TODO Auto-generated method stub
		return null;
	}
}
