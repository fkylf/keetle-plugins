package com.horizonio.kettle.trans;

import cn.benma666.kettle.mytuils.KettleUtils;
import cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import redis.clients.jedis.Jedis;

/**
 * Created by horizon on 2017/7/11.
 * 通用的发送序列化为json的数据到redis中
 */
public class CommonRedisJsonSender extends EasyExpandRunBase {

    private Jedis jedis = null;
    private String redisListName = null;

    /**
     * 具体处理每一行数据
     *
     * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#disposeRow(java.lang.Object[])
     */
    @Override
    protected void disposeRow(Object[] outputRow) throws KettleException {
        //设置JOB名称
        outputRow[getFieldIndex("JOB_NAME")] = KettleUtils.getRootJobName(ku);


        RowMetaInterface fields = data.outputRowMeta;
        String[] fieldNames = fields.getFieldNames();

        JSONObject jsonObject = new JSONObject();

        for (int i = 0; i < fieldNames.length; i++) {
            jsonObject.put(fieldNames[i], outputRow[i]);
        }

        String newField = jsonObject.toJSONString();
        if (newField != null && !newField.isEmpty()) {
            jedis.lpush(redisListName, newField);
        }


    }

    /**
     * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#init()
     */
    @Override
    protected void init() {
        ku.logBasic("初始化插件");
        String host = configInfo.getString("redisHost");
        int port = configInfo.getInteger("port").intValue();
        String password = configInfo.getString("password");
        redisListName = configInfo.getString("redisListName");
        jedis = new Jedis(host, port);
        jedis.auth(password);
    }

    /**
     * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#end()
     */
    @Override
    protected void end() {
        ku.logBasic("数据处理结束");
        if (jedis != null) {
            jedis.close();
        }
    }

    /**
     * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#getDefaultConfigInfo(org.pentaho.di.trans.TransMeta, java.lang.String)
     */
    @Override
    public String getDefaultConfigInfo(TransMeta transMeta, String stepName) throws Exception {
        //创建一个JSON对象，用于构建配置对象，避免直接拼字符串构建JSON字符串
        JSONObject params = new JSONObject();
        //设置一个参数key1
        params.put("key1", "");
        params.put("key2", "");
        RowMetaInterface fields = transMeta.getPrevStepFields(stepName);
        if (fields.size() == 0) {
            throw new RuntimeException("没有获取到上一步骤的字段，请确认连接好上一步骤");
        }
        params.put("PrevInfoFields", fields.toString());
        //创建一个JSON数组对象，用于存放数组参数
        JSONArray arr = new JSONArray();
        arr.add("arr1");
        arr.add("arr2");
        params.put("array", arr);
        //生成的参数样例
        //{
        //  "array":[
        //          "arr1",
        //          "arr2"
        //  ],
        //  "key1":""
        //}
        //返回格式化后的默认JSON配置参数，供使用者方便快捷的修改配置
        return JSON.toJSONString(params, true);
    }

    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        //本步骤新添加输出到下一步的字段（r中默认携带上一步的所有字段）
        addField(r, "MY_JOB_NAME", ValueMeta.TYPE_STRING, ValueMeta.TRIM_TYPE_BOTH, origin, "JOB名称");
    }

}
