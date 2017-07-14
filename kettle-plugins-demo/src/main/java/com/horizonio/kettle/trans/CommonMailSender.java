package com.horizonio.kettle.trans;

import cn.benma666.kettle.mytuils.KettleUtils;
import cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.horizonio.mail.MailTransmitResult;
import com.horizonio.mail.SimpleMailTransmitter;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

/**
 * Created by horizon on 2017/7/11.
 * 用于统一发送邮件的kettle转换
 */
public class CommonMailSender extends EasyExpandRunBase {

    /**
     * 收件人字段
     */
    String toInputField;

    /**
     * subject
     */
    String subjectField;

    /**
     * 内容模板
     */
    String contentTemplate;

    /**
     * 内容占位
     */
    JSONArray contentFieldArray;

    /**
     * 具体处理每一行数据
     *
     * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#disposeRow(java.lang.Object[])
     */
    @Override
    protected void disposeRow(Object[] outputRow) {
        //设置JOB名称
        outputRow[getFieldIndex("JOB_NAME")] = KettleUtils.getRootJobName(ku);

        String to = outputRow[getFieldIndex(toInputField)].toString();
        String subject = outputRow[getFieldIndex(subjectField)].toString();
        //依次用jsonarr中的数据替换模板中的占位符
        for (int i = 0; i < contentFieldArray.size(); i++) {

            while (contentTemplate.contains("{" + i + "}")) {
                contentTemplate = contentTemplate.replace("{" + i + "}", outputRow[getFieldIndex(contentFieldArray.get(i).toString())].toString());
            }
        }

        //发送邮件
        SimpleMailTransmitter simpleMailTransmitter = new SimpleMailTransmitter(subject, contentTemplate, to);
        simpleMailTransmitter.run();

        MailTransmitResult mailTransmitResult = simpleMailTransmitter.getSendResult();
        if (mailTransmitResult.isSuccess()) {

        } else {
            ku.logError(mailTransmitResult.toString());
        }

    }

    /**
     * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#init()
     */
    @Override
    protected void init() {
        ku.logBasic("初始化插件");

        contentFieldArray = configInfo.getJSONArray("contentFieldArray");
        toInputField = configInfo.getString("toInputField");
        contentTemplate = configInfo.getString("contentTemplate");
        subjectField = configInfo.getString("subjectField");
    }

    /**
     * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#end()
     */
    @Override
    protected void end() {
        ku.logBasic("数据处理结束");
    }

    /**
     * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#getDefaultConfigInfo(org.pentaho.di.trans.TransMeta, java.lang.String)
     */
    @Override
    public String getDefaultConfigInfo(TransMeta transMeta, String stepName) throws Exception {
        //创建一个JSON对象，用于构建配置对象，避免直接拼字符串构建JSON字符串
        JSONObject params = new JSONObject();
        //设置一个参数key1
        params.put("toInputField", "");
        params.put("contentTemplate", "尊敬的{0}，您好。您的账户已经开通成功");
        params.put("subjectField", "");

        RowMetaInterface fields = transMeta.getPrevStepFields(stepName);
        if (fields.size() == 0) {
            throw new RuntimeException("没有获取到上一步骤的字段，请确认连接好上一步骤");
        }
        params.put("PrevInfoFields", fields.toString());
        //创建一个JSON数组对象，用于存放数组参数
        JSONArray arr = new JSONArray();
        arr.add("item1");
        arr.add("item2");
        params.put("contentFieldArray", arr);
        //生成的参数样例
        //{
        //  "array":[
        //          "item1",
        //          "item2"
        //  ],
        //  "toInputField":"",
        //  "contentTemplate":"",
        //  "subjectField":""
        //}
        //返回格式化后的默认JSON配置参数，供使用者方便快捷的修改配置
        return JSON.toJSONString(params, true);
    }

    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        //添加输出到下一步的字段
        addField(r, "MY_JOB_NAME", ValueMeta.TYPE_STRING, ValueMeta.TRIM_TYPE_BOTH, origin, "JOB名称");
    }

}