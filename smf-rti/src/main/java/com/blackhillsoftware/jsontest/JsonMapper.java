package com.blackhillsoftware.jsontest;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;	
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

public class JsonMapper<T> 
{
	public JsonMapper()
	{
		objectMapper = new ObjectMapper()
				.registerModule(new ParameterNamesModule())
				.registerModule(new Jdk8Module())
				.registerModule(new JavaTimeModule());
	}
	
	public JsonMapper<T> add(String key, Function<? super T, ? extends Object> valueGetter)
	{
		if (getters.containsKey(key))
		{
			throw new IllegalArgumentException("Duplicate key : " + key);
		}
		getters.put(key, valueGetter);
		return this;
	}
	
	private Map<String, Function<? super T, ? extends Object>> getters = new LinkedHashMap<>();
	private ObjectMapper objectMapper;
	
	public String genJson(T sourceObject) throws JsonProcessingException
	{
		Map<String, Object> entries = new LinkedHashMap<>();
				
		for (Entry<String, Function<? super T, ? extends Object>> entry : getters.entrySet())
		{
			entries.put(entry.getKey(), entry.getValue().apply(sourceObject));
		}
		
		return objectMapper.writeValueAsString(entries);
	}
}
