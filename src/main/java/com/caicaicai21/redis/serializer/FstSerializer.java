package com.caicaicai21.redis.serializer;

import org.nustaq.serialization.FSTConfiguration;

import com.caicaicai21.redis.DistributedRedisUtils.ISerializer;

public class FstSerializer implements ISerializer {

	private FSTConfiguration config = FSTConfiguration.createDefaultConfiguration();

	public FstSerializer() {

	}

	public FstSerializer(FSTConfiguration config) {
		this.config = config;
	}

	@Override
	public byte[] serialize(Object object) {
		if (config == null || object == null)
			return null;
		return config.asByteArray(object);
	}

	@Override
	public Object deserialize(byte[] data) {
		if (config == null || data == null)
			return null;
		return config.asObject(data);
	}
}
