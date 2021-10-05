package com.smfreports.cics;

import java.io.*;
import java.math.BigInteger;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import com.blackhillsoftware.smf.*;
import com.blackhillsoftware.smf.cics.*;
import com.blackhillsoftware.smf.cics.monitoring.*;
import com.blackhillsoftware.smf.cics.monitoring.Dictionary;
import com.blackhillsoftware.smf.cics.monitoring.fields.*;

public class Transaction2JsonAll 
{
	static Set<String> excludeFields = new HashSet<>(Arrays.asList(
			 "RMIDB2"
			,"RMIDBCTL"
			,"RMIEXDLI"
			,"RMIMQM"
			,"RMICPSM"
			,"RMITCPIP"
			,"RMITOTAL"
			,"RMIOTHER"
			));
	
    public static void main(String[] args) throws IOException 
    {
        if (args.length < 1)
        {
            System.out.println("Usage: Transaction2Json <input-name> <input-name2> ...");
            System.out.println("<input-name> can be filename, //DD:DDNAME or //'DATASET.NAME'");          
            return;
        }
        
        int noDictionary = 0;
        int txCount = 0;

        // SmfRecordReader.fromName(...) accepts a filename, a DD name in the
        // format //DD:DDNAME or MVS dataset name in the form //'DATASET.NAME'

        ObjectMapper objectMapper = JsonMapper.builder()
                .addModule(new ParameterNamesModule())
                .addModule(new Jdk8Module())
                .addModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .build();
        
        // Keep a list of fields specific to each dictionary
        Map<Dictionary, List<MonitoringField>> dictionaryFields = new HashMap<>();
        
        for (String name : args)
        {
            try (SmfRecordReader reader = SmfRecordReader.fromName(name)) 
            {            
                reader.include(110, 1);
                for (SmfRecord record : reader) 
                {
                    Smf110Record r110 = Smf110Record.from(record);
                    
                    if (r110.haveDictionary()) 
                    {
                        Dictionary dictionary = r110.dictionary();
                        
                        // Get the list of fields for the dictionary 
                        List<MonitoringField> fields = dictionaryFields.get(dictionary);
                        
                        // Create the list of fields if we don't have one already
                        if (fields == null) 
                        {                           
                            fields = Field.values().stream() 						    // start with all possible values
                                    .filter(x -> r110.dictionary().haveEntry(x)) 		// if they are in the dictionary
                                    .filter(x -> !excludeFields.contains(x.getName()))  // and not specifically excluded
                                    .collect(Collectors.toList());
                            dictionaryFields.put(dictionary, fields);
                         }
                                                
                        String applid = r110.mnProductSection().smfmnprn();
                        
                        for (PerformanceRecord txData : r110.performanceRecords()) 
                        {       
                        	txCount++;
                        	// Get the data for the list of fields
                            Map<String, Object> entries = extractFields(fields, applid, txData);
                            
                            // Generate and write out the JSON data
                            System.out.println(
                            		objectMapper.writeValueAsString(entries));
                        }
                    } 
                    else 
                    {
                        noDictionary++;
                    }
                }         
            }
        }
                
        System.out.format(
                "%n%nTotal Transactions: %,d%n", 
                txCount);
               
        if (noDictionary > 0) 
        {
            System.out.format(
                    "%n%nSkipped %,d records because no applicable dictionary was found.", 
                    noDictionary);
        }
        
        if (Smf110Record.getCompressedByteCount() > 0) 
        {
            System.out.format(
                    "%n%nCompressed bytes %,d, decompressed bytes %,d, compression %.1f%%.", 
                    Smf110Record.getCompressedByteCount(),
                    Smf110Record.getDecompressedByteCount(),
                    (double)(Smf110Record.getDecompressedByteCount() - Smf110Record.getCompressedByteCount()) / Smf110Record.getDecompressedByteCount() * 100);
        }
    }

	private static Map<String, Object> extractFields(
			List<MonitoringField> fields, 
			String applid,
			PerformanceRecord txData) 
	{
		Map<String, Object> entries = new LinkedHashMap<>();                
		entries.put("Applid", applid);
		
		for (MonitoringField entry : fields)
		{
		    try 
		    {
		    	// Special handling
		    	switch (entry.getName())
		    	{
		    	case "NETUOWSX":
		    	case "OTRANFLG":
		    	case "TRNGRPID":
					entries.put(entry.getName(), txData.getFieldAsHex(entry));
		    		break;
		    	default:
		    		if (entry instanceof ClockField)
		    		{
		    			Map<String, Object> subentries = new LinkedHashMap<>();
		    			CicsClock clock = txData.getField( (ClockField)entry );
		    			// exclude if all zeros
		    			if (!(clock.periodCount() == 0 && clock.flags() == 0 && clock.timer().equals(Duration.ZERO)))
		    			{
		        			subentries.put("TIMER", clock.timer());
		        			subentries.put("FLAGS", String.format("0x%02X", clock.flags()));
		        			subentries.put("COUNT", clock.periodCount());
		                    entries.put(entry.getName(), subentries);
		    			}
		    		}
		    		else if (entry instanceof ByteStringField)
		    		{
		    			String value = txData.getField( (ByteStringField)entry );
		    			// exclude if all zeros
		    			if (!(value.length() == 0))
		    			{
		                    entries.put(entry.getName(), value);
		    			}
		    		}
		    		else if (entry instanceof CountField)
		    		{
		    			BigInteger value = txData.getFieldAsBigInteger( (CountField)entry );
		    			// exclude if all zeros
		    			if (!(value.equals(BigInteger.ZERO)))
		    			{
		                    entries.put(entry.getName(), value);
		    			}
		    		}
		    		else
		    		{
		    			entries.put(entry.getName(), txData.getField(entry));
		    		}
		    	}
		    	
		    }
		    catch (Exception e) 
		    {
		        System.err.format("Exception reading field: %s%n%s%n", entry.getName(), e.getMessage());
		    }
		}
		return entries;
	}
}
