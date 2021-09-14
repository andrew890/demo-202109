package com.blackhillsoftware.test;

import java.util.List;

import com.blackhillsoftware.smf.SmfRecord;

public class RecordStats
{
    /**
     * Initialize statistics for a new record type/subtype combination.
     * 
     */
    RecordStats(int recordType, int subType)
    {
    	typeSubtype = new TypeSubtype(recordType, subType);
    }
    
    public static RecordStats from(List<RecordStats> entries)
    {
    	if (entries == null) throw new IllegalArgumentException("Entries cannot be null");
    	if (entries.size() == 0) throw new IllegalArgumentException("Entry count must be greater than 0");
    	
    	RecordStats result = new RecordStats(entries.get(0).getRecordtype(), entries.get(0).getSubtype());
    	for (RecordStats entry : entries)
    	{
            result.count += entry.count;
            result.bytes += entry.bytes;
            result.minLength = Math.min(result.minLength, entry.minLength);
            result.maxLength = Math.max(result.maxLength, entry.maxLength);
    	}
    	return result;
    }

    /**
     * Add a record to the statistics
     * 
     * @param record a SMF record
     */
    public void add(SmfRecord record)
    {
        count++;
        int length = record.recordLength();
        bytes += length;
        minLength = Math.min(length, minLength);
        maxLength = Math.max(length, maxLength);
    }
    
    private TypeSubtype typeSubtype;
    private int  count = 0;
    private long bytes = 0;
    private int  maxLength = 0;
    private int  minLength = Integer.MAX_VALUE;

    TypeSubtype getTypeSubtype() { return typeSubtype; }
    int getRecordtype()	{ return typeSubtype.getSmfType(); }
    int getSubtype() 	{ return typeSubtype.getSubType(); }
    int getCount() 		{ return count; }
    long getBytes() 	{ return bytes; }
    int getMaxLength() 	{ return maxLength; }
    int getMinLength()	{ return minLength; }    
}
