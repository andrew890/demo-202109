package com.blackhillsoftware.test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import com.blackhillsoftware.smf.SmfRecord;
import com.blackhillsoftware.smf.realtime.MissedDataEvent;
import com.blackhillsoftware.smf.realtime.SmfConnection;
import com.ibm.jzos.MvsCommandCallback;
import com.ibm.jzos.MvsConsole;
import com.ibm.jzos.WtoConstants;

public class RecordCount
{
	public static void main(String[] args) throws IOException, InterruptedException 
	{
		Map<LocalDateTime, Map<TypeSubtype, RecordStats>> statsByDateTime = new HashMap<>();
			    
		try (SmfConnection rti = 
				SmfConnection.resourceName(args[0])
					.onMissedData(RecordCount::handleMissedData)
					.connect())
		{	
			if (!MvsConsole.isListening()) {
				MvsConsole.startMvsCommandListener();
			}
			MvsConsole.registerMvsCommandCallback(new CommandHandler(rti, statsByDateTime));
			
			for (byte[] record : rti)
			{
				SmfRecord smfrec = new SmfRecord(record);
				LocalDateTime minute = smfrec.smfDateTime().truncatedTo(ChronoUnit.MINUTES);
				synchronized(statsByDateTime)
				{
					Map<TypeSubtype, RecordStats> minuteEntry = statsByDateTime.get(minute);
					if (minuteEntry == null)
					{
						minuteEntry = new HashMap<>();
						statsByDateTime.put(minute, minuteEntry);
						
						// new minute - remove timed out entries
						
						List<LocalDateTime> toRemove = 
								statsByDateTime.keySet().stream()
								.filter(entry -> entry.isBefore(minute.plusMinutes(-59)))
								.collect(Collectors.toList());
						for (LocalDateTime entry : toRemove)
						{
							statsByDateTime.remove(entry);
						}
					}
					
					TypeSubtype key = new TypeSubtype(
						smfrec.recordType(), 
						smfrec.hasSubtypes() ? smfrec.subType() : 0);
				
					RecordStats stats = minuteEntry.get(key);
					if (stats == null)
					{
						stats = new RecordStats(key.getSmfType(), key.getSubType());
						minuteEntry.put(key, stats);
					}
					stats.add(smfrec);
				}
			}
		} 	
	}	
	
	static void handleMissedData(MissedDataEvent e)
	{
		System.out.println("Missed Data!");
		e.throwException(false);	
	}
		
	static class CommandHandler implements MvsCommandCallback
	{
	    private Map<LocalDateTime, Map<TypeSubtype, RecordStats>> statsMap;
	    private SmfConnection connection = null;
		
		CommandHandler(SmfConnection connection, Map<LocalDateTime, Map<TypeSubtype, RecordStats>> statsMap)
		{
			this.connection = connection;
			this.statsMap = statsMap;
		}
	
		@Override
		public void handleModify(String arg0) {
			if (!arg0.equals("REPORT"))
			{
				MvsConsole.wto("Valid modify commands: REPORT",
						WtoConstants.ROUTCDE_PROGRAMMER_INFORMATION,
						WtoConstants.DESC_IMPORTANT_INFORMATION_MESSAGES);
			}
			else
			{
				synchronized (statsMap)
				{
					Optional<LocalDateTime> lastminute = statsMap.keySet().stream().max(LocalDateTime::compareTo);
					
					if (!lastminute.isPresent())
					{
						wtoLine("No Data");
						return;
					}
					
					LocalDateTime fiveMins = lastminute.get().minusMinutes(5);
					LocalDateTime sixtyMins = lastminute.get().minusMinutes(60);
					
					List<Map<TypeSubtype, RecordStats>> lastminuteEntry = statsMap.entrySet().stream()
							.filter(entry -> entry.getKey().equals(lastminute.get()))
							.map(entry -> entry.getValue())
							.collect(Collectors.toList());
					
					List<Map<TypeSubtype, RecordStats>> last5MinuteEntries = statsMap.entrySet().stream()
							.filter(entry -> entry.getKey().isAfter(fiveMins))
							.map(entry -> entry.getValue())
							.collect(Collectors.toList());
					
					List<Map<TypeSubtype, RecordStats>> last60MinuteEntries = statsMap.entrySet().stream()
							.filter(entry -> entry.getKey().isAfter(sixtyMins))
							.map(entry -> entry.getValue())
							.collect(Collectors.toList());
							
					wtoLine("Last Minute");
					writeReport(lastminuteEntry);
					wtoLine("");
					wtoLine("Last 5 Minutes");
					writeReport(last5MinuteEntries);
					wtoLine("");
					wtoLine("Last 60 Minutes");
					writeReport(last60MinuteEntries);
				}
			}
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
		
	    private static void writeReport(List<Map<TypeSubtype, RecordStats>> statsByDateTime)
	    {
	    	// Flatten maps
	    	
	    	List<RecordStats> allentries = statsByDateTime.stream()
	    		.flatMap(minute -> minute.entrySet().stream())
	    		.map(minuteTypeSubtype -> minuteTypeSubtype.getValue())
	    		.collect(Collectors.toList());

	        // get the total bytes from all record types
	        long totalbytes = allentries.stream() 		 
	            .mapToLong(entry -> entry.getBytes()) // Value is RecordStats entry 
	            .sum();                                          // Sum total bytes for all records
	        
	    	// Group by TypeSubtype
	    	
	        Map<TypeSubtype, List<RecordStats>> byTypeSubtype = 
	        	allentries.stream()
       			.collect(Collectors.groupingBy(entry -> entry.getTypeSubtype()));  	

	        // write heading
	        wtoLine(String.format("%5s %8s %8s %6s %6s %6s", 
	            "Type", "Subtype", "Records", "MB", "Pct", "Avg"));

	        // write data
	        byTypeSubtype.values().stream()
	        	.map(typeSubTypeEntries -> RecordStats.from(typeSubTypeEntries))
	            .sorted(Comparator.comparingLong(RecordStats::getBytes).reversed())

	            // alternative sort, by type and subtype
	            // .sorted(Comparator
	            // .comparingInt(RecordStats::getRecordtype)
	            // .thenComparingInt(RecordStats::getSubtype))

	            .forEachOrdered(entry -> 
	            	wtoLine(String.format("%5d %8d %8d %6d %6.1f %6d", 
	                    entry.getRecordtype(),
	                    entry.getSubtype(), 
	                    entry.getCount(), 
	                    entry.getBytes() / (1024 * 1024),              
	                    (float) (entry.getBytes()) / totalbytes * 100, 
	                    entry.getBytes() / entry.getCount())));
	    }
		
	    private static void wtoLine(String line)
	    {
			MvsConsole.wto(line,
					WtoConstants.ROUTCDE_PROGRAMMER_INFORMATION,
					WtoConstants.DESC_JOB_STATUS);
	    }	
	}

}
