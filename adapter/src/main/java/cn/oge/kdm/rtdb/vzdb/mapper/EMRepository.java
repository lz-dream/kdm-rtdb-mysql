package cn.oge.kdm.rtdb.vzdb.mapper;

import cn.oge.kdm.data.domain.value.RtValue;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface EMRepository {
//    @Select(value = "select case when date_time is null then 0 else date_time end as time," +
//            "case when ${field} is null then '' else ${field} end as value " +
//            "from ${tableName} order by date_Time desc limit 1")
    @Select(value = "select date_time as time," +
            "${field}  as value " +
            "from ${tableName} order by date_Time desc limit 1")
    RtValue getValue(@Param(value = "field")String field,@Param(value = "tableName")String tableName);
    @Select(value = "select ${field} as value from ${tableName} where date_time = ${dateTime}")
    Object getValueByTime(@Param(value = "field")String field,@Param(value = "tableName")String tableName,
                           @Param(value = "dateTime")Long dateTime);
    @Select(value = "select CONCAT(LOWER(column_name),\"_\",SUBSTR(TABLE_NAME,16)) as code from information_schema.COLUMNS " +
            "where TABLE_SCHEMA='cmsdb' and TABLE_NAME = #{tableName} and data_type != 'blob'")
    List<String> getFields(@Param(value = "tableName")String tableName);

    @Select(value = "select CONCAT(LOWER(column_name),\"_\",SUBSTR(TABLE_NAME,16)) as code from information_schema.COLUMNS " +
            "where TABLE_SCHEMA='cmsdb' and TABLE_NAME = #{tableName} and data_type = 'blob'")
    List<String> getBlobFields(@Param(value = "tableName")String tableName);
}
