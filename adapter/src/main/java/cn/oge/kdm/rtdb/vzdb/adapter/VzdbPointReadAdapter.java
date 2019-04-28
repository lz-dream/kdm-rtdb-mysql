package cn.oge.kdm.rtdb.vzdb.adapter;

import cn.oge.kdm.data.consts.RtDataState;
import cn.oge.kdm.data.domain.data.RtHistoryData;
import cn.oge.kdm.data.domain.data.RtSnapshotData;
import cn.oge.kdm.data.domain.data.RtValueData;
import cn.oge.kdm.data.domain.value.RtValue;
import cn.oge.kdm.rtdb.adapter.RtdbPointReadAdapter;
import cn.oge.kdm.rtdb.vzdb.mapper.EMRepository;
import lombok.AllArgsConstructor;

import java.util.*;
import java.util.regex.Pattern;

@AllArgsConstructor
public class VzdbPointReadAdapter implements RtdbPointReadAdapter {
    private final String[] tables = {"external_master0101", "external_master0102", "external_master0201",
            "external_master0202", "external_master0301", "external_master0302"};
    private EMRepository emRepository;

    @Override
    public Map<String, RtSnapshotData> readLatestData(Collection<String> codes) {
        Map<String, RtSnapshotData> result = new HashMap<>(codes.size());
        if (codes.contains("ALL")){
            codes.clear();
            for (String table : tables) {
                codes.addAll(emRepository.getFields(table));
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
                List<String> fields = emRepository.getFields(tableName);
                if (!fields.contains(code)) {
                    result.put(code, new RtSnapshotData(RtDataState.DS_NO_ALIAS));
                } else {
                    RtSnapshotData rtSnapshotData = new RtSnapshotData();
                    rtSnapshotData.setCode(code);
                    RtValue rtValue = emRepository.getValue(field, tableName);
                    rtSnapshotData.setData(rtValue);
                    result.put(code, rtSnapshotData);
                }
            }
        }
        return result;
    }

    @Override
    public Map<String, RtValueData> readDataByTime(Collection<String> codes, Date time) {
        long longTime = time.getTime();
        Map<String, RtValueData> result = new HashMap<>(codes.size());
        if (codes.contains("ALL")){
            codes.clear();
            for (String table : tables) {
                codes.addAll(emRepository.getFields(table));
            }
        }
        for (String code : codes) {
            String pattern = "([A-Za-z0-9]+)_([0-9]{4})";
            boolean isMatch = Pattern.matches(pattern, code);
            if (!isMatch) { // 无别名
                result.put(code, new RtValueData(RtDataState.DS_NO_ALIAS));
            } else {
                String[] split = code.split("_");
                String field = split[0];
                String tableName = "external_master" + split[1];
                List<String> fields = emRepository.getFields(tableName);
                if (!fields.contains(code)) {
                    result.put(code, new RtValueData(RtDataState.DS_NO_ALIAS));
                } else {
                    RtValueData rtValueData = new RtValueData();
                    rtValueData.setCode(code);
                    Object rtValue = emRepository.getValueByTime(field, "external_master" + tableName, longTime);
                    rtValueData.setData(rtValue);
                    result.put(code, rtValueData);
                }
            }
        }
        return result;
    }

    @Override
    public Map<String, RtHistoryData> readHistoryData(Collection<String> collection, Date date, Date date1, int i) {
        return null;
    }

}
