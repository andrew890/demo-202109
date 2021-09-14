package com.blackhillsoftware.test;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import com.blackhillsoftware.smf.SmfRecord;
import com.blackhillsoftware.smf.SmfRecordReader;
import com.ibm.jzos.ZUtil;

public class SmfCopy
{
    public static void main(String[] args) throws IOException, InterruptedException
    {   	
    	LocalDateTime firstRecordTime = null;
    	LocalDateTime previousRecordTime = null;

    	LocalDateTime startTime = LocalDateTime.now();
    	
    	try (SmfRecordReader reader = SmfRecordReader.fromDD("INPUT"))
        { 
            for (SmfRecord record : reader.include(110,1))
            {
            	if (firstRecordTime == null)
            	{
            		firstRecordTime = record.smfDateTime();
            		previousRecordTime = firstRecordTime.truncatedTo(ChronoUnit.MINUTES);
            	}
            	            	
            	if (!record.smfDateTime().truncatedTo(ChronoUnit.MINUTES).equals(previousRecordTime))
            	{
            		previousRecordTime = record.smfDateTime().truncatedTo(ChronoUnit.MINUTES);
            		
            		Duration smfRecordTime =  Duration.between(firstRecordTime, record.smfDateTime());           		
            		Duration programTime = Duration.between(startTime, LocalDateTime.now());
            		
            		while (programTime.getSeconds() < smfRecordTime.getSeconds() / 60)
            		{
            			Thread.sleep(500);
                   		programTime = Duration.between(startTime, LocalDateTime.now());
            		}
            	}
            	
            	if (record.recordLength() > 24)
            	{
            		ZUtil.smfRecord(252, 1, record.getBytes());
            	}
            }
        }
    }
}
