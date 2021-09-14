package com.blackhillsoftware.test;

import java.io.IOException;
import java.util.*;

import com.ibm.jzos.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import com.blackhillsoftware.smf.*;
import com.blackhillsoftware.smf.cics.Smf110Record;
import com.blackhillsoftware.smf.cics.monitoring.PerformanceRecord;
import com.blackhillsoftware.smf.cics.monitoring.fields.Field;
import com.blackhillsoftware.smf.realtime.*;

public class Cics2Json
{
	public static void main(String[] args) throws IOException 
	{		
        try (SmfRecordReader reader = SmfRecordReader.fromDD("CICSDICT").include(110, 1)) 
        {
            for (SmfRecord record : reader)
            {
            	// creating the 110 record loads the dictionary
            	Smf110Record.from(record);
            }
        }
		
        int noDictionary = 0;
        int txCount = 0;
        
		try (SmfConnection rti = 
				SmfConnection
					.resourceName(args[0])
					.onMissedData(x -> handleMissedData(x))
					.connect())		
		{	
			if (!MvsConsole.isListening()) {
				MvsConsole.startMvsCommandListener();
			}
			MvsConsole.registerMvsCommandCallback(new CommandHandler(rti));

	        ObjectMapper objectMapper = JsonMapper.builder()
	                .addModule(new ParameterNamesModule())
	                .addModule(new Jdk8Module())
	                .addModule(new JavaTimeModule())
	                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
	                .build();
			
			for (byte[] record : rti)
			{
				if (SmfRecord.recordType(record) == 252) // for testing
				{
					record[5] = 110;			
				}
				if (SmfRecord.recordType(record) == 110 && SmfRecord.subType(record) == 1)
				{
					Smf110Record r110 = Smf110Record.from(record);
	                if (r110.haveDictionary()) 
	                {                                               
	                    String applid = r110.mnProductSection().smfmnprn();
	                    
	                    for (PerformanceRecord txData : r110.performanceRecords()) 
	                    {
	                    	txCount++;
	                    	
	                        Map<String, Object> entries = new LinkedHashMap<>();                
	                        entries.put("Applid", applid);
	                        
	                        entries.put("TRAN",     txData.getField(            Field.TRAN));
	                        entries.put("PGMNAME",  txData.getField(            Field.PGMNAME));
	                        entries.put("START",    txData.getField(            Field.START));
	                        entries.put("STOP",     txData.getField(            Field.STOP));
	                        entries.put("USRDISPT", txData.getFieldTimerSeconds(Field.USRDISPT));
	                        entries.put("DISPWTT",  txData.getFieldTimerSeconds(Field.DISPWTT));
	                        entries.put("USRCPUT",  txData.getFieldTimerSeconds(Field.USRCPUT));
	                        
	                        System.out.println(objectMapper.writeValueAsString(entries));
	                    }
	                } 
	                else 
	                {
	                    noDictionary++;
	                }		
				}
			}
		// Connection closed & resources freed automatically at the end of the try block 	
		}
		
        System.err.format(
                "%n%nTotal Transactions: %,d%n", 
                txCount);
               
        if (noDictionary > 0) 
        {
            System.err.format(
                    "%n%nSkipped %,d records because no applicable dictionary was found.", 
                    noDictionary);
        }
        
        if (Smf110Record.getCompressedByteCount() > 0) 
        {
            System.err.format(
                    "%n%nCompressed bytes %,d, decompressed bytes %,d, compression %.1f%%.", 
                    Smf110Record.getCompressedByteCount(),
                    Smf110Record.getDecompressedByteCount(),
                    (double)(Smf110Record.getDecompressedByteCount() - Smf110Record.getCompressedByteCount()) / Smf110Record.getDecompressedByteCount() * 100);
        }
	}	
	
	static void handleMissedData(MissedDataEvent e)
	{
		System.err.println("Missed Data!");
		e.throwException(false);	
	}
		
	static class CommandHandler implements MvsCommandCallback
	{
	    private SmfConnection connection = null;
		
		CommandHandler(SmfConnection connection)
		{
			this.connection = connection;
		}
	
		@Override
		public void handleModify(String arg0) 
		{
			MvsConsole.wto("Modify not implemented",
					WtoConstants.ROUTCDE_PROGRAMMER_INFORMATION,
					WtoConstants.DESC_IMPORTANT_INFORMATION_MESSAGES);
		}

		@Override
		public void handleStart(String arg0) {
			// do nothing
		}

		@Override
		public boolean handleStop() 
		{
			if (connection != null)
			{
				try {
					connection.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return false;
		}
	}
}
