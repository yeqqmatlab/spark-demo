package com.zsy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by yqq on 2019/5/31.
 */
public class HiveStudyMain {

    public static void main(String[] args) throws Exception {
        //加载驱动，创建连接
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        /**
         *
         *<property>
         *    <name>hive.server2.authentication</name>
         *    <value>CUSTOM</value>
         *        <description>
         *        Expects one of [nosasl, none, ldap, kerberos, pam, custom].
         *        Client authentication types.
         *        NONE: no authentication check
         *        LDAP: LDAP/AD based authentication
         *        KERBEROS: Kerberos/GSSAPI authentication
         *        CUSTOM: Custom authentication provider
         *         (Use with property hive.server2.custom.authentication.class)
         *        PAM: Pluggable authentication module
         *        NOSASL:  Raw transport
         *   </description>
         *</property>
         *
         */
        Connection conn = DriverManager.getConnection("jdbc:hive2://ip244:10000/zsy_warehouse", "test", "123456");
        Statement st = conn.createStatement();

        //查询
        String sql = "select * from exam_student limit 100";
        ResultSet rs = st.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString("student_id"));
        }
    }

}
