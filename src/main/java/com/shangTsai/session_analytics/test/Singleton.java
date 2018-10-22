package com.shangTsai.session_analytics.test;
/**
 * 
 * Singleton demo - class instance can be instantiate only once
 * @author shangluntsai
 * 1. class constructor must be private
 * 2. instantiate through a static method getInstance()
 * 3. use case: config
 */
public class Singleton {

	// declare a class instance
	private static Singleton instance  = null;
	
	// private constructor
	private Singleton() {
		
	}
	
	public static Singleton getInstance() {
		// 2-step checking
		if(instance == null) {
			// Synchronizing multiple threads 
			// making sure only one thread at a time can obtain the lock to proceed further
			synchronized(Singleton.class) {
				// the thread with lock will found the instance is null, and create a new instance
				// the next thread with lock will found the instance is NOT null and keep waiting
				if(instance == null) {
					instance = new Singleton();
				}
			}
		}
		return instance;
	}
}
