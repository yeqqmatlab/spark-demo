package com.java.config;

import com.google.common.base.Strings;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 * <p>
 * 1、配置管理组件可以复杂，也可以很简单，对于简单的配置管理组件来说，只要开发一个类，可以在第一次访问它的
 * 时候，就从对应的properties文件中，读取配置项，并提供外界获取某个配置key对应的value的方法
 * 2、如果是特别复杂的配置管理组件，那么可能需要使用一些软件设计中的设计模式，比如单例模式、解释器模式
 * 可能需要管理多个不同的properties，甚至是xml类型的配置文件
 * 3、我们这里的话，就是开发一个简单的配置管理组件，就可以了
 *
 * @author Administrator
 */
public class ConfigurationManager {

    /**
     * 配置读取类
     */
    private final static Properties prop = new Properties();

    /**
     * 静态代码块
     *
     * Java中，每一个类第一次使用的时候，就会被Java虚拟机（JVM）中的类加载器，去从磁盘上的.class文件中
     * 加载出来，然后为每个类都会构建一个Class对象，就代表了这个类
     *
     * 每个类在第一次加载的时候，都会进行自身的初始化，那么类初始化的时候，会执行哪些操作的呢？
     * 就由每个类内部的static {}构成的静态代码块决定，我们自己可以在类中开发静态代码块
     * 类第一次使用的时候，就会加载，加载的时候，就会初始化类，初始化类的时候就会执行类的静态代码块
     *
     * 因此，对于我们的配置管理组件，就在静态代码块中，编写读取配置文件的代码
     * 这样的话，第一次外界代码调用这个ConfigurationManager类的静态方法的时候，就会加载配置文件中的数据
     *
     * 而且，放在静态代码块中，还有一个好处，就是类的初始化在整个JVM生命周期内，有且仅有一次，也就是说
     * 配置文件只会加载一次，然后以后就是重复使用，效率比较高；不用反复加载多次
     */
    static {
        try (InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("config.properties")) {
            // 通过一个“类名.class”的方式，就可以获取到这个类在JVM中对应的Class对象
            // 然后再通过这个Class对象的getClassLoader()方法，就可以获取到当初加载这个类的JVM
            // 中的类加载器（ClassLoader），然后调用ClassLoader的getResourceAsStream()这个方法
            // 就可以用类加载器，去加载类加载路径中的指定的文件
            // 最终可以获取到一个，针对指定文件的输入流（InputStream）

            // 调用Properties的load()方法，给它传入一个文件的InputStream输入流
            // 即可将文件中的符合"key=value"格式的配置项，都加载到Properties对象中
            // 加载过后，此时，Properties对象中就有了配置文件中所有的key-value对了
            // 然后外界其实就可以通过Properties对象获取指定key对应的value
            prop.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取配置文件
     *
     * @param key 键
     * @return
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }


    /**
     * 获取配置文件,没有就返回指定值
     *
     * @param key          键
     * @param defaultValue 指定值
     * @return
     */
    public static String getProperty(String key, String defaultValue) {
        String value = prop.getProperty(key.trim());
        if (Strings.isNullOrEmpty(value)) {
            value = defaultValue;
        }
        return value.trim();
    }


    /**
     * 获取整数类型的配置项
     *
     * @param key
     * @return value
     */
    public static Integer getInteger(String key) {
        return Integer.valueOf(getProperty(key));
    }

    /**
     * 获取布尔类型的配置项
     *
     * @param key
     * @return value
     */
    public static Boolean getBoolean(String key) {
        return Boolean.valueOf(getProperty(key));
    }

    /**
     * 获取Long类型的配置项
     *
     * @param key
     * @return
     */
    public static Long getLong(String key) {
        return Long.valueOf(getProperty(key));
    }
}

