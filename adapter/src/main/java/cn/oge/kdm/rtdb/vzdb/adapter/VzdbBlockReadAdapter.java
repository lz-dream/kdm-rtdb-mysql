package cn.oge.kdm.rtdb.vzdb.adapter;

import cn.oge.kdm.data.consts.RtDataState;
import cn.oge.kdm.data.domain.data.RtSnapshotData;
import cn.oge.kdm.data.domain.data.RtTimesData;
import cn.oge.kdm.data.domain.data.RtValueData;
import cn.oge.kdm.data.domain.value.RtValue;
import cn.oge.kdm.rtdb.adapter.RtdbBlockReadAdapter;
import cn.oge.kdm.rtdb.vzdb.common.Base64Util;
import cn.oge.kdm.rtdb.vzdb.common.MysqlBlobUtil;
import cn.oge.kdm.rtdb.vzdb.mapper.EMRepository;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

@AllArgsConstructor
public class VzdbBlockReadAdapter implements RtdbBlockReadAdapter {
        private final String[] tables = {"external_master0101", "external_master0102", "external_master0201",
            "external_master0202", "external_master0301", "external_master0302"};
//    private final String[] tables = {"external_master0102"};
    private EMRepository emRepository;

    @Override
    public Map<String, RtSnapshotData> readLatestData(Collection<String> codes) {
        Map<String, RtSnapshotData> result = new HashMap<>(codes.size());
        if (codes.contains("ALL")) {
            codes.clear();
            for (String table : tables) {
                codes.addAll(emRepository.getBlobFields(table));
            }
        }
        for (String code : codes) {
            String pattern = "([A-Za-z0-9]+)_([0-9]{4})";
            boolean isMatch = Pattern.matches(pattern, code);
            if (!isMatch) { // 无别名
                result.put(code, new RtSnapshotData(RtDataState.DS_NO_ALIAS));
            } else {
                String[] split = code.split("_");
                String field = split[0];
                String tableName = "external_master" + split[1];
                List<String> fields = emRepository.getBlobFields(tableName);
                if (!fields.contains(code)) {
                    result.put(code, new RtSnapshotData(RtDataState.DS_NO_ALIAS));
                } else {
                    RtValue rtValue = emRepository.getValue(field, tableName);
                    if (rtValue == null) {
                        result.put(code, new RtSnapshotData(RtDataState.DATA_INVALID));
                    } else {
                        RtSnapshotData rtSnapshotData = new RtSnapshotData();
                        rtSnapshotData.setCode(code);
                        byte[] buf = rtValue.getBlobValue();
                        if (buf == null || buf.length == 0) {
                            result.put(code, new RtSnapshotData(RtDataState.DATA_INVALID));
                        } else {
                            try {
                                byte[] byteCompress = MysqlBlobUtil.byteCompress(buf);
                                String encodeValue = Base64Util.encode(byteCompress);
                                rtValue.setValue(encodeValue);
                                rtSnapshotData.setData(rtValue);
                                result.put(code, rtSnapshotData);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    @Override
    public Map<String, RtTimesData> readHistoryTimes(Collection<String> collection, Date date, Date date1) {
        return null;
    }

    @Override
    public Map<String, RtValueData> readDataByTime(Collection<String> collection, Date date) {
        long longTime = date.getTime();
        Map<String, RtValueData> result = new HashMap<>(collection.size());
        if (collection.contains("ALL")) {
            collection.clear();
            for (String table : tables) {
                collection.addAll(emRepository.getBlobFields(table));
            }
        }
        for (String code : collection) {
            String pattern = "([A-Za-z0-9]+)_([0-9]{4})";
            boolean isMatch = Pattern.matches(pattern, code);
            if (!isMatch) { // 无别名
                result.put(code, new RtValueData(RtDataState.DS_NO_ALIAS));
            } else {
                String[] split = code.split("_");
                String field = split[0];
                String tableName = "external_master" + split[1];
                List<String> fields = emRepository.getBlobFields(tableName);
                if (!fields.contains(code)) {
                    result.put(code, new RtValueData(RtDataState.DS_NO_ALIAS));
                } else {
                    RtValueData rtValueData = new RtValueData();
                    rtValueData.setCode(code);
                    Object rtValue = emRepository.getValueByTime(field, tableName, longTime);
                    byte[] buf = (byte[]) rtValue;
                    if (buf == null || buf.length == 0) {
                        result.put(code, new RtValueData(RtDataState.DATA_INVALID));
                    } else {
                        try {
                            byte[] byteCompress = MysqlBlobUtil.byteCompress(buf);
                            String encodeValue = Base64Util.encode(byteCompress);
                            rtValueData.setData(encodeValue);
                            result.put(code, rtValueData);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        return result;
    }
}
