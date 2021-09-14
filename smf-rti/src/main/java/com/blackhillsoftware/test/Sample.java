package com.blackhillsoftware.test;

import java.io.IOException;

import com.blackhillsoftware.smf.realtime.MissedDataEvent;
import com.blackhillsoftware.smf.realtime.SmfConnection;
import com.ibm.jzos.MvsCommandCallback;
import com.ibm.jzos.MvsConsole;
import com.ibm.jzos.WtoConstants;

public class Sample
{
	public static void main(String[] args) throws IOException 
	{		    
		System.out.println("This sample does nothing with the records - issue STOP command to end.");

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
			
			for (byte[] record : rti)
			{
				// byte array is a SMF record - do whatever is required.
			}
		// Connection closed & resources freed automatically at the end of the try block 	
		} 	
	}	
	
	static void handleMissedData(MissedDataEvent e)
	{
		System.out.println("Missed Data!");
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
			MvsConsole.wto("Mmodify not implemented",
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
