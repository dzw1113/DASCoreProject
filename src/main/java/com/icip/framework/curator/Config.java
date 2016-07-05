package com.icip.framework.curator;

public interface Config {

	byte[] getConfig(String path) throws Exception;
}
