package com.blackhillsoftware.test;

public class TypeSubtype 	
{
	int smfType;
	int smfSubType;	
	
	TypeSubtype(int smfType, int smfSubType)
	{
		this.smfType = smfType;
		this.smfSubType = smfSubType;
	}
	
	int getSmfType()
	{
		return smfType;
	}
	
	int getSubType()
	{
		return smfSubType;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + smfSubType;
		result = prime * result + smfType;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TypeSubtype other = (TypeSubtype) obj;
		if (smfSubType != other.smfSubType)
			return false;
		if (smfType != other.smfType)
			return false;
		return true;
	}
}	