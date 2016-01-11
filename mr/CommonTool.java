package my.hadoop.simulator.mr;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CommonTool {
	
	private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = 
	    new ConcurrentHashMap<Class<?>, Constructor<?>>();
	private static final Class<?>[] EMPTY_ARRAY = new Class[]{};
	
	
	public static <T> T newInstance(Class<T> theClass) {
	    T result;
	    try {
	      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
	      if (meth == null) {
	        meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
	        meth.setAccessible(true);
	        CONSTRUCTOR_CACHE.put(theClass, meth);
	      }
	      result = meth.newInstance();
	    } catch (Exception e) {
	      throw new RuntimeException(e);
	    }
	    return result;
	  }
	
	

}
