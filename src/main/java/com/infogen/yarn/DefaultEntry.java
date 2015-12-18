package com.infogen.yarn;

import java.util.Map.Entry;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年8月26日 下午5:14:29
 * @since 1.0
 * @version 1.0
 */
public class DefaultEntry<K, V> implements Entry<K, V> {
	private final K key;
	private V value;

	public DefaultEntry(final K key, final V value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public K getKey() {
		return key;
	}

	@Override
	public V getValue() {
		return value;
	}

	@Override
	public V setValue(final V value) {
		this.value = value;
		return value;
	}

}
