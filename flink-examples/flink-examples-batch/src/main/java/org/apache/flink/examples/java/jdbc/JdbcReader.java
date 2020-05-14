package org.apache.flink.examples.java.jdbc;

//mysql table:
//create table tb
//(
//id BIGINT UNSIGNED auto_increment comment '自增主键'
//primary key,
//cooper BIGINT(19) null ,
//
//user_sex VARCHAR(2) null
//
//);

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class JdbcReader extends RichSourceFunction<Tuple2<String,String>> {
    private static final Logger logger = LoggerFactory.getLogger(JdbcReader.class);

    private Connection connection = null;
    private PreparedStatement ps = null;

    //该方法主要用于打开数据库连接，下面的ConfigKeys类是获取配置的类
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动
        connection = DriverManager.getConnection("mysql://localhost:3306", "xiaoxing", "123456");//获取连接
        ps = connection.prepareStatement("select * from tb");
    }

    //执行查询并获取结果
    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        try {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String name = resultSet.getString("nick");
                String id = resultSet.getString("user_id");
                logger.error("readJDBC name:{}", name);
                Tuple2<String,String> tuple2 = new Tuple2<>();
                tuple2.setFields(id,name);
                ctx.collect(tuple2);//发送结果，结果是tuple2类型，2表示两个元素，可根据实际情况选择
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }

    }

    //关闭数据库连接
    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }
}