package com.smfreports.cics;

import java.io.*;
import java.util.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import com.blackhillsoftware.smf.*;
import com.blackhillsoftware.smf.cics.*;
import com.blackhillsoftware.smf.cics.monitoring.*;
import com.blackhillsoftware.smf.cics.monitoring.fields.*;

public class Transactions2Json 
{
    public static void main(String[] args) throws IOException 
    {
        if (args.length < 1)
        {
            System.out.println("Usage: Transactions2Json <input-name> <input-name2> ...");
            System.out.println("<input-name> can be filename, //DD:DDNAME or //'DATASET.NAME'");          
            return;
        }
        
        int noDictionary = 0;
        int txCount = 0;

        // Setup Jackson JSON object mapper
        ObjectMapper objectMapper = JsonMapper.builder()
                .addModule(new ParameterNamesModule())
                .addModule(new Jdk8Module())
                .addModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .build();
                       
        for (String name : args)
        {
            // SmfRecordReader.fromName(...) accepts a filename, a DD name in the
            // format //DD:DDNAME or MVS dataset name in the form //'DATASET.NAME'
            
            try (SmfRecordReader reader = SmfRecordReader.fromName(name).include(110, 1)) 
            {     
                
                for (SmfRecord record : reader) 
                {
                    Smf110Record r110 = Smf110Record.from(record);
                    
                    if (r110.haveDictionary()) 
                    {                                               
                        String applid = r110.mnProductSection().smfmnprn();
                        
                        for (PerformanceRecord txData : r110.performanceRecords()) 
                        {
                            // LinkedHashMap should result in the JSON fields output in the order they are put in 
                            Map<String, Object> entries = new LinkedHashMap<>();
                            
                            entries.put("Applid", applid);
                            
                            entries.put("TRAN",     txData.getField(            Field.TRAN));
                            entries.put("PGMNAME",  txData.getField(            Field.PGMNAME));
                            entries.put("START",    txData.getField(            Field.START));
                            entries.put("STOP",     txData.getField(            Field.STOP));
                            entries.put("USRDISPT", txData.getFieldTimerSeconds(Field.USRDISPT));
                            entries.put("DISPWTT",  txData.getFieldTimerSeconds(Field.DISPWTT));
                            entries.put("USRCPUT",  txData.getFieldTimerSeconds(Field.USRCPUT));
                            
                            // use object mapper to write JSON   
                            System.out.println(objectMapper.writeValueAsString(entries));
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
}
