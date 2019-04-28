package cn.oge.kdm.rtdb.vzdb.common;

import cn.oge.wave.decompress.WaveDataModelCompress;
import cn.oge.wave.decompress.model.WaveDataHeader;
import cn.oge.wave.decompress.model.WaveDataModel;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@Slf4j
public class MysqlBlobUtil {

    /**
     * 将mysql数据库中的blob数据转换成kdm要求的格式
     * @param b 原数组
     * @return 转换后的byte数组
     * @throws IOException
     */
    public static byte[] byteCompress(byte[] b) throws IOException {
        WaveDataHeader header = new WaveDataHeader();
        byte[] compressbyte = null;

        ByteBuffer buf = ByteBuffer.wrap(b);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        //time
        int bufTime = buf.getInt(0);
        //header.setTime(bufTime);
        //flag
        header.setRecordType(buf.getShort(4));
        //period
        header.setPeriod(buf.getShort(6));
        //keyPhaseCount
        header.setKeyPhaseCount(buf.getShort(8));
        //keyPhaseOffset
        int keyPhaseOffset[] = new int[header.getKeyPhaseCount()];
        for (int i = 0; i < header.getKeyPhaseCount(); i++) {
            keyPhaseOffset[i] = buf.getInt(10+i*4);
        }
        header.setKeyPhaseOffset(keyPhaseOffset);
        //dataNum
        header.setWaveNum(buf.getInt(512+10));

//        log.error("TIME: "+bufTime+"; Flag: "+header.getRecordType()+"; 周期数: "+header.getPeriod()+
//                ";键相数目: "+header.getKeyPhaseCount()+";波形数据: "+header.getWaveNum());

        float data[] = new float[header.getWaveNum()];
        for (int i = 0; i < header.getWaveNum(); i++) {
            data[i] = buf.getInt(512+14+i*4);
        }
        //float[] -> double[]
        double[] ware = new double[header.getWaveNum()];
        for (int i = 0; i < header.getWaveNum(); i++) {
            ware[i] = Double.parseDouble(String.valueOf(data[i]));
        }
        try {
            compressbyte = compress(header, ware);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return compressbyte;
    }
    private static byte[] compress(WaveDataHeader header, double[] waves) throws Exception {
        WaveDataModel waveModel = new WaveDataModel();
        waveModel.setHeader(header);
        waveModel.setData(waves);
        return WaveDataModelCompress.executeNew(waveModel);
    }
}
