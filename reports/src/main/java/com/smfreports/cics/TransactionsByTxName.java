package com.smfreports.cics;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.blackhillsoftware.smf.*;
import com.blackhillsoftware.smf.cics.*;
import com.blackhillsoftware.smf.cics.monitoring.*;
import com.blackhillsoftware.smf.cics.monitoring.fields.*;

/**
 * Report CICS transactions by Transaction name and applid.
 *
 */
public class TransactionsByTxName 
{
    public static void main(String[] args) throws IOException 
    {
        if (args.length < 1)
        {
            System.out.println("Usage: TransactionsByTxName <input-name> <input-name2> ...");
            System.out.println("<input-name> can be filename, //DD:DDNAME or //'DATASET.NAME'");          
            return;
        }
        
        Map<TransactionsByTxNameKey, TransactionData> transactions = new HashMap<>();

        int noDictionary = 0;
        int txCount = 0;

        // SmfRecordReader.fromName(...) accepts a filename, a DD name in the
        // format //DD:DDNAME or MVS dataset name in the form //'DATASET.NAME'
        
        for (String name : args)
        {
            try (SmfRecordReader reader = SmfRecordReader.fromName(name)
                    .include(110, 1)) 
            {     
                for (SmfRecord record : reader) 
                {
                    Smf110Record r110 = Smf110Record.from(record);
                    
                    if (r110.haveDictionary()) 
                    {
                        String applid = r110.mnProductSection().smfmnprn(); // get applid for record

                        for (PerformanceRecord txData : r110.performanceRecords()) // foreach transaction 
                        {
                            txCount++;
                            String txName = txData.getField(Field.TRAN);
                                                  
                            TransactionsByTxNameKey key = new TransactionsByTxNameKey(applid, txName);
                         
                            // create a new TransactionData entry if necessary, and add this transaction
                            transactions.computeIfAbsent(key, x -> new TransactionData())
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
        
        writeReport(transactions);
        
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

    private static void writeReport(Map<TransactionsByTxNameKey, TransactionData> transactions) 
    {
        // transactions are keyed by applid and transaction name
        // group by transaction name, giving a list of entries for each applid with that transaction
        Map<String, List<Entry<TransactionsByTxNameKey, TransactionData>>> transactionsByName 
            = transactions.entrySet()
                .stream()
                .collect(Collectors.groupingBy(
                        entry -> entry.getKey().getTransaction()));    
        
        // sort groups by key (transaction name) then process each
        transactionsByName.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEachOrdered(transaction -> 
            {                
                // Headings
                System.out.format("%n%-8s", transaction.getKey());
    
                System.out.format("%n%-8s %15s %15s %15s %15s %15s %15s%n%n", 
                        "Applid", 
                        "Count", 
                        "Avg Elapsed", 
                        "CPU", 
                        "Avg CPU", 
                        "Avg Disp.", 
                        "Avg Disp Wait");
    
                // stream values (1 or more applid entries for this transaction)
                // and sort by applid
                transaction.getValue().stream()
                    .sorted((a,b) -> a.getKey().getApplid()
                                .compareTo(b.getKey().getApplid()))
                    .forEachOrdered(txInfo -> 
                    {
                        // write detail line
                        System.out.format("%-8s %15d %15f %15f %15f %15f %15f%n", 
                                txInfo.getKey().getApplid(),
                                txInfo.getValue().getCount(), 
                                txInfo.getValue().getAvgElapsed(), 
                                txInfo.getValue().getCpu(),
                                txInfo.getValue().getAvgCpu(), 
                                txInfo.getValue().getAvgDispatch(),
                                txInfo.getValue().getAvgDispatchWait());
                    });
            });

    }    
}
