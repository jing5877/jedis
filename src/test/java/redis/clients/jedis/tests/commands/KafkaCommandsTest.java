package redis.clients.jedis.tests.commands;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

public class KafkaCommandsTest {
	
	private static String host = "127.0.0.1";
	private static int port = 8066;
	private static String topic = "test6";
	
	protected Jedis jedis;

	public KafkaCommandsTest() {
	    super();
	}

	@Before
	public void setUp() throws Exception {
	    jedis = new Jedis(host, port, 5000);
	    jedis.connect();
	    jedis.auth("pwd04");
	    jedis.flushAll();
	}

	@After
	public void tearDown() {
	    jedis.disconnect();
	}
	
	@Test
	public void testKpush() {
		for(int i = 0; i< 5;i++) {
			List<String> rtn = jedis.kpush(topic, String.valueOf(i));
			printList(rtn);
		}
	}
	
	@Test
	public void testKpop1() {
		for(int i = 0; i< 5;i++) {
			List<String> rtn = jedis.kpop(topic);
			printList(rtn);
		}
	}
	
	@Test
	public void testKpop2() {
		
		for(int partition = 0; partition<3;partition++) {
			for(int offset = 0; offset< 5;offset++) {
				List<String> rtn = jedis.kpop(topic, partition, offset);
				printList(rtn);
			}
		}
	}
	
	
	
	@Test
	public void testKpop3() {
		for(int partition = 0; partition<3;partition++) {
			for(int offset = 0; offset< 5;offset++) {
				List<String> rtn = jedis.kpop(topic, partition, offset, 1024);
				printList(rtn);
			}
		}
	}
	
	@Test
	public void testKconsumeoffset() {
		
		String rtn = jedis.kconsumeoffset(topic, 1);
		System.out.println(rtn);
		
	}
	
	@Test
	public void testKreturnoffset() {
		
		String rtn = jedis.kreturnoffset(topic, 1, 0);
		System.out.println(rtn);
	}
	
	
	public void printList(List<String> strs) {
		
		if(strs == null) {
			System.out.println("strs is null.");
			return;
		}
		
		for(String str : strs) {
			System.out.print(str + " - ");
		}
		System.out.println();
	}
	  
	  

}
