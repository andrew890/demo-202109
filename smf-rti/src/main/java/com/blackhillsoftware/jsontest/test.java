package com.blackhillsoftware.jsontest;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.blackhillsoftware.smf.SmfRecordReader;
import com.blackhillsoftware.smf.smf30.*;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 *
 * Search all SMF records for a text string, 
 * and print Time, System, Type and Subtype for 
 * each record found.
 *
 */
public class test                                                                            
{                                                                                               
    public static void main(String[] args) throws IOException                                   
    {
        if (args.length < 2)
        {
            System.out.println("Usage: SmfTextSearch string <input-name>");
            System.out.println("<input-name> can be filename, //DD:DDNAME or //'DATASET.NAME'");          	
            return;
        }
        
        JsonMapper<Smf30Record> mapper = new JsonMapper<Smf30Record>()
        		.add("System", record -> record.system())
        		.add("Jobname", record -> record.identificationSection().smf30jbn().toString())
        		.add("Time", record -> record.smfDateTime().toString())
        		.add("DDNAMES", record -> record.excpSections()
        				.stream()
        				.map(entry -> entry.smf30ddn())
        				.distinct()
        				.collect(Collectors.toList())				
        			)
        		.add("DDINFO", record -> Calc.DDInfo(record.excpSections()))
        		;

        String searchString = args[0];

        // SmfRecordReader.fromName(...) accepts a filename, a DD name in the
        // format //DD:DDNAME or MVS dataset name in the form //'DATASET.NAME'
    	
        try (SmfRecordReader reader 
        		= SmfRecordReader.fromName(args[1])
        			.include(30,5))
        { 
            reader
            .stream()
            // Optionally filter records 
            //.filter(record -> record.recordType() != 14) // Exclude type 14 (read dataset)
            //.filter(record -> record.toString().contains(searchString))
            .limit(10000)
            .forEach(record ->
	            {
	            	try {
						System.out.println(mapper.genJson(Smf30Record.from(record)));
					} catch (JsonProcessingException e) {
						e.printStackTrace();
					}
	            }
            	);

            System.out.format("Done");                  
        }                                   
    }                          
    
    static class Calc
    {
    	static Map<String, Object> DDInfo(List<ExcpSection> excpSections)
    	{
    		Map<String, Object> result = new LinkedHashMap<>();
    		
    		Map<String, List<ExcpSection>> ddnames = excpSections.stream()
    			.filter(entry -> entry.smf30xbs() != 0 || entry.smf30blk() != 0)
    			.collect(Collectors.groupingBy(entry -> entry.smf30ddn()));
    		
    		for (Entry<String, List<ExcpSection>> ddname : ddnames.entrySet())
    		{
    			result.put("DDNAME", ddname.getKey());
    			result.put("BLKSIZE", ddname.getValue().stream().mapToLong(ExcpSection::smf30xbs).max());
    			result.put("BLOCKS", ddname.getValue().stream().mapToLong(ExcpSection::smf30blk).sum());    			
    		}
    		return result;
    	}
    }
    
    
}                                                                                               