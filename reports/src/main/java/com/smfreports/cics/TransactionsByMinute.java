package com.smfreports.cics;

import java.io.*;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;    
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import com.blackhillsoftware.smf.*;
import com.blackhillsoftware.smf.cics.*;
import com.blackhillsoftware.smf.cics.monitoring.*;
import com.blackhillsoftware.smf.cics.monitoring.fields.*;

public class TransactionsByMinute 
{
    public static void main(String[] args) throws IOException 
    {
        if (args.length < 1)
        {
            System.out.println("Usage: TransactionsByMinute <input-name> <input-name2> ...");
            System.out.println("<input-name> can be filename, //DD:DDNAME or //'DATASET.NAME'");          
            return;
        }
        
        Map<TransactionKey, TransactionData> dataByMinute = new HashMap<>();

        int noDictionary = 0;
        int txCount = 0;

        // SmfRecordReader.fromName(...) accepts a filename, a DD name in the
        // format //DD:DDNAME or MVS dataset name in the form //'DATASET.NAME'
        
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
                        String applid = r110.mnProductSection().smfmnprn();
                            
                        for (PerformanceRecord txData : r110.performanceRecords()) 
                        {
                            txCount++;
                            dataByMinute
                                .computeIfAbsent(
                                        new TransactionKey(applid, txData), transactions -> new TransactionData())
                                .add(txData);
                        }
                    } 
                    else 
                    {
                        noDictionary++;
                    }
                }
            }
        }
        
        writeReport(dataByMinute);
        
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

    private static void writeReport(Map<TransactionKey, TransactionData> transactions) throws JsonProcessingException 
    {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule());
                
        for (Entry<TransactionKey, TransactionData> entry : transactions.entrySet())
        {
            Map<String, Object> entries = new LinkedHashMap<>();                
            entries.put("Minute", entry.getKey().getMinute().toString());
            entries.put("Applid", entry.getKey().getApplid());
            entries.put("Transaction", entry.getKey().getTransaction());
            entries.put("Count", entry.getValue().getCount());
            entries.put("Avg Elapsed", entry.getValue().getAvgElapsed());
            entries.put("CPU", entry.getValue().getCpu());
            entries.put("Avg CPU", entry.getValue().getAvgCpu());
            entries.put("Avg Disp.", entry.getValue().getAvgDispatch());
            entries.put("Avg Disp Wait", entry.getValue().getAvgDispatchWait());
            
            System.out.println(objectMapper.writeValueAsString(entries));
        }
    }
    
    private static class TransactionKey
    {
        public TransactionKey(String applid, PerformanceRecord record)
        {
            this.applid = applid;
            transaction = record.getField(Field.TRAN);
            this.minute = record.getField(Field.STOP).truncatedTo(ChronoUnit.MINUTES);
        }
        
        public ZonedDateTime getMinute() 
        {
            return minute;
        }
        public String getApplid() 
        {
            return applid;
        }
        public String getTransaction() 
        {
            return transaction;
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(applid, minute, transaction);
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (!(obj instanceof TransactionKey))
                return false;
            TransactionKey other = (TransactionKey) obj;
            return Objects.equals(applid, other.applid) 
                    && Objects.equals(minute, other.minute)
                    && Objects.equals(transaction, other.transaction);
        }
        
        private ZonedDateTime minute;
        private String applid;
        private String transaction;
    }
    
    private static class TransactionData 
    {
        public void add(PerformanceRecord txData) 
        {
            count++;
            elapsed += Utils.ToSeconds(
                    Duration.between(txData.getField(Field.START), 
                            txData.getField(Field.STOP)));
            dispatch += txData.getField(Field.USRDISPT).timerSeconds();
            dispatchWait += txData.getField(Field.DISPWTT).timerSeconds();
            cpu += txData.getField(Field.USRCPUT).timerSeconds();
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

        private int count = 0;
        private double elapsed = 0;
        private double dispatch = 0;
        private double dispatchWait = 0;
        private double cpu = 0;
    }
}
