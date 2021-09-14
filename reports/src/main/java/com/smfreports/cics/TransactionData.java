package com.smfreports.cics;

import java.time.Duration;

import com.blackhillsoftware.smf.Utils;
import com.blackhillsoftware.smf.cics.monitoring.PerformanceRecord;
import com.blackhillsoftware.smf.cics.monitoring.fields.Field;

/**
 * Class to collect information for a group of transactions.
 * How transactions are grouped is controlled by the using class.  
 *
 */
public class TransactionData 
{
    private int count = 0;
    private double elapsed = 0;
    private double dispatch = 0;
    private double dispatchWait = 0;
    private double cpu = 0;
    
    public TransactionData() 
    {
    }

    public void add(PerformanceRecord txData) 
    {
        count++;
        elapsed += Utils.ToSeconds(
                Duration.between(txData.getField(Field.START), txData.getField(Field.STOP)));
        dispatch += txData.getFieldTimerSeconds(Field.USRDISPT);
        dispatchWait += txData.getFieldTimerSeconds(Field.DISPWTT);
        cpu += txData.getFieldTimerSeconds(Field.USRCPUT);
    }

    public int getCount() 
    {
        return count;
    }

    public double getCpu() 
    {
        return cpu;
    }

    public Double getAvgElapsed() 
    {
        return count != 0 ? elapsed / count : null;
    }

    public Double getAvgDispatch() 
    {
        return count != 0 ? dispatch / count : null;
    }

    public Double getAvgDispatchWait() 
    {
        return count != 0 ? dispatchWait / count : null;
    }

    public Double getAvgCpu() 
    {
        return count != 0 ? cpu / count : null;
    }
}
