package com.inspur;

import com.inspur.model.RruInfo;
import org.apache.log4j.Logger;
//import org.apache.logging.log4j.core.Logger;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/**
 * @author Li-Xiaoxu
 * @version 1.0
 * @date 2020/7/17 17:41
 */
public class Main {
    private static Logger logger = Logger.getLogger(Main.class);
    private static Connection connection = null;
    private static ArrayList<RruInfo> rruInfos = new ArrayList<RruInfo>();
    private static ArrayList<RruInfo> rruInfoAfters = new ArrayList<RruInfo>();


    public static void main(String[] args) throws SQLException {
        String vendorId = "";
        String provinceId = "";
        String timeStamp = "";
        init();
        if(args.length == 3){
            vendorId = args[0];
            provinceId = args[1];
            timeStamp = args[2];

            logger.info("正在处理的厂家为：" + vendorId);
            logger.info("正在处理的省份为：" + provinceId);
            logger.info("正在处理的日期为：" + timeStamp);

            process(vendorId, provinceId, timeStamp);

        } else if(args.length == 2){
            vendorId = args[0];
            timeStamp = args[1];
            logger.info("正在处理所有省份");
            logger.info("正在处理的厂家为：" + vendorId);
            logger.info("正在处理的日期为：" + timeStamp);
            process(vendorId, timeStamp);
        } else {
            logger.info("输入参数错误");
            return;
        }

        connection.close();
    }


    public static void init() throws SQLException {
        connection = DriverManager.getConnection("jdbc:oracle:thin:@10.162.65.192:10110:wonop1", "lternop", "P0ibojja");
        if (connection == null) {
            logger.info("数据库连接失败");
        } else {
            logger.info("数据库连接成功");
        }
    }

    public static void process(String vendorId, String timeStamp) throws SQLException {
        String rruSql = "SELECT oid,rru_id,vendor_id,province_id,time_stamp,related_cell\n" +
                "  FROM ne_rru_l\n" +
                " where vendor_id = ?\n" +
                "   and time_stamp = to_date(?, 'yyyy-mm-dd')\n";
        logger.info("执行查询rru信息的SQL为：" + rruSql);
        PreparedStatement ps = connection.prepareStatement(rruSql);
        ps.setInt(1,Integer.parseInt(vendorId));
        ps.setString(2,timeStamp);
        ResultSet rs = ps.executeQuery();
        logger.info("查询完毕，正在对数据进行处理：");
        rruInfos.clear();
        while (rs.next()){
            RruInfo rruInfo = new RruInfo();
            rruInfo.setOid(rs.getString("oid"));
            rruInfo.setRruId(rs.getString("rru_id"));
            rruInfo.setCellId(rs.getString("related_cell"));
            rruInfo.setProvinceId(rs.getInt("province_id"));
            rruInfo.setTimeStamp(rs.getTimestamp("time_stamp"));

            rruInfos.add(rruInfo);
        }
        ps.close();
        logger.info("获取到RRU的数量为" + rruInfos.size());
        logger.info("进行数据拆分处理");
        for (RruInfo rruInfo : rruInfos) {
            if(rruInfo.getCellId() != null && rruInfo.getCellId().contains("|")){
                String[] split = rruInfo.getCellId().split("\\|");
                for (int i = 0; i < split.length; i++) {
                    RruInfo rruInfo1 = new RruInfo();
                    rruInfo1.setOid(rruInfo.getOid());
                    rruInfo1.setProvinceId(rruInfo.getProvinceId());
                    rruInfo1.setRruId(rruInfo.getRruId());
                    rruInfo1.setTimeStamp(rruInfo.getTimeStamp());
                    rruInfo1.setCellId(split[i]);

                    rruInfoAfters.add(rruInfo1);
                }

            }else {
                rruInfoAfters.add(rruInfo);
            }
        }
        logger.info("拆分后的数据数量为：" + rruInfoAfters.size());

        //进行拆分后的插入操作，插入前先删除
        logger.info("插入前先删除");
        String deleteSql = "delete from ne_rru_l_hw_divided\n" +
                " where vendor_id = ?\n" +
                "   and time_stamp = to_date(?, 'yyyy-mm-dd')";
        logger.info("执行删除数据的SQL为：" + deleteSql);
        PreparedStatement ps1 = connection.prepareStatement(deleteSql);
        ps1.setInt(1,Integer.parseInt(vendorId));
        ps1.setString(2, timeStamp);
        int count = ps1.executeUpdate();
        ps1.close();
        logger.info("删除的数据条数为：" + count);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        String insertSql = "insert into ne_rru_l_hw_divided(oid, rru_id, vendor_id, province_id, related_cell, time_stamp, insert_time)\n" +
                "values(?,?,?,?,?,to_date(?,'yyyy-MM-dd'), to_date(?, 'yyyy-MM-dd HH24:mi:ss'))";
        logger.info("执行插入数据的SQL为：" + insertSql);
        PreparedStatement ps2 = connection.prepareStatement(insertSql);
        count = 0;
        for (RruInfo rruInfoAfter : rruInfoAfters) {
            ps2.setString(1,rruInfoAfter.getOid());
            ps2.setString(2,rruInfoAfter.getRruId());
            ps2.setInt(3,Integer.parseInt(vendorId));
            ps2.setInt(4,rruInfoAfter.getProvinceId());
            ps2.setString(5,rruInfoAfter.getCellId());
            try {
                ps2.setString(6,simpleDateFormat.format(rruInfoAfter.getTimeStamp()));
            } catch (SQLException e) {
                e.printStackTrace();
            }
            ps2.setString(7,simpleDateFormat1.format(new Date()));

//            try {
//                ps2.executeUpdate();
//            } catch (SQLException e) {
//                e.printStackTrace();
//                logger.info("异常RRU的信息为：" + rruInfoAfter.getOid());
//            }

            ps2.addBatch();

            count++;

        }
        ps2.executeBatch();
        ps2.clearBatch();
        ps2.close();
        logger.info("数据插入完毕，插入" + count + "条。");

        connection.commit();

    }

    public static void process(String vendorId, String provinceId, String timeStamp) throws SQLException {
        String rruSql = "SELECT oid,rru_id,vendor_id,province_id,time_stamp,related_cell\n" +
                "  FROM ne_rru_l\n" +
                " where vendor_id = ?\n" +
                "   and province_id = ?\n" +
                "   and time_stamp = to_date(?, 'yyyy-mm-dd')\n";
        logger.info("执行查询rru信息的SQL为：" + rruSql);
        PreparedStatement ps = connection.prepareStatement(rruSql);
        ps.setInt(1,Integer.parseInt(vendorId));
        ps.setInt(2,Integer.parseInt(provinceId));
        ps.setString(3,timeStamp);
        ResultSet rs = ps.executeQuery();
        logger.info("查询完毕，正在对数据进行处理：");
        rruInfos.clear();
        while (rs.next()){
            RruInfo rruInfo = new RruInfo();
            rruInfo.setOid(rs.getString("oid"));
            rruInfo.setRruId(rs.getString("rru_id"));
            rruInfo.setCellId(rs.getString("related_cell"));
            rruInfo.setProvinceId(rs.getInt("province_id"));
            rruInfo.setTimeStamp(rs.getTimestamp("time_stamp"));

            rruInfos.add(rruInfo);
        }
        ps.close();
        logger.info("获取到RRU的数量为" + rruInfos.size());
        logger.info("进行数据拆分处理");
        for (RruInfo rruInfo : rruInfos) {
            if(rruInfo.getCellId() != null && rruInfo.getCellId().contains("|")){
                String[] split = rruInfo.getCellId().split("\\|");
                for (int i = 0; i < split.length; i++) {
                    RruInfo rruInfo1 = new RruInfo();
                    rruInfo1.setOid(rruInfo.getOid());
                    rruInfo1.setProvinceId(rruInfo.getProvinceId());
                    rruInfo1.setRruId(rruInfo.getRruId());
                    rruInfo1.setTimeStamp(rruInfo.getTimeStamp());
                    rruInfo1.setCellId(split[i]);

                    rruInfoAfters.add(rruInfo1);
                }

            }else {
                rruInfoAfters.add(rruInfo);
            }
        }
        logger.info("拆分后的数据数量为：" + rruInfoAfters.size());

        //进行拆分后的插入操作，插入前先删除
        logger.info("插入前先删除");
        String deleteSql = "delete from ne_rru_l_hw_divided\n" +
                " where vendor_id = ?\n" +
                "   and province_id = ?\n" +
                "   and time_stamp = to_date(?, 'yyyy-mm-dd')";
        logger.info("执行删除数据的SQL为：" + deleteSql);
        PreparedStatement ps1 = connection.prepareStatement(deleteSql);
        ps1.setInt(1,Integer.parseInt(vendorId));
        ps1.setInt(2,Integer.parseInt(provinceId));
        ps1.setString(3, timeStamp);
        int count = ps1.executeUpdate();
        ps1.close();
        logger.info("删除的数据条数为：" + count);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        String insertSql = "insert into ne_rru_l_hw_divided(oid, rru_id, vendor_id, province_id, related_cell, time_stamp, insert_time)\n" +
                "values(?,?,?,?,?,to_date(?,'yyyy-MM-dd'), to_date(?, 'yyyy-MM-dd HH24:mi:ss'))";
        logger.info("执行插入数据的SQL为：" + insertSql);
        PreparedStatement ps2 = connection.prepareStatement(insertSql);
        count = 0;
        for (RruInfo rruInfoAfter : rruInfoAfters) {
            ps2.setString(1,rruInfoAfter.getOid());
            ps2.setString(2,rruInfoAfter.getRruId());
            ps2.setInt(3,Integer.parseInt(vendorId));
            ps2.setInt(4,rruInfoAfter.getProvinceId());
            ps2.setString(5,rruInfoAfter.getCellId());
            try {
                ps2.setString(6,simpleDateFormat.format(rruInfoAfter.getTimeStamp()));
            } catch (SQLException e) {
                e.printStackTrace();
            }
            ps2.setString(7,simpleDateFormat1.format(new Date()));

//            try {
//                ps2.executeUpdate();
//            } catch (SQLException e) {
//                e.printStackTrace();
//                logger.info("异常RRU的信息为：" + rruInfoAfter.getOid());
//            }

            ps2.addBatch();

            count++;

        }
        ps2.executeBatch();
        ps2.clearBatch();
        ps2.close();
        logger.info("数据插入完毕，插入" + count + "条。");

        connection.commit();

    }

}
