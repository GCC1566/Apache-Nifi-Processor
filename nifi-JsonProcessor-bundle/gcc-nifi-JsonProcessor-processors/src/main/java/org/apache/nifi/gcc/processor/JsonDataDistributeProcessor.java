package org.apache.nifi.gcc.processor;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.gcc.ruledistribute.Ruler;
import org.apache.nifi.gcc.ruledistribute.logicrule.FieldValueRuler;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JSONDistributeProcessor
 * @author GCC
 */

@SideEffectFree
@Tags({"JsonDataDistribute","com.gc"})
@CapabilityDescription("Divide data according to configuration")
public class JsonDataDistributeProcessor extends AbstractProcessor{

    //分发管道：规则对应器
    private static Map<Relationship, Ruler> distributers = new ConcurrentHashMap<>();

    //定义数据异常处理方式，默认为丢弃至Failure关系
    public static String[] VALID_FAIL_STRATEGY = {"rollback", "transfer to failure"};
    public static final PropertyDescriptor FAIL_STRATEGY = new PropertyDescriptor.Builder()
            .name("distribute-failure-strategy")
            .displayName("Failure strategy")
            .description("What to do with unhandled exceptions. If you want to manage exception by code then keep the default value `rollback`."
                    +" If `transfer to failure` selected and unhandled exception occurred then all flowFiles received from incoming queues in this session"
                    +" will be transferred to `failure` relationship with additional attributes set: ERROR_MESSAGE and ERROR_STACKTRACE."
                    +" If `rollback` selected and unhandled exception occurred then all flowFiles received from incoming queues will be penalized and returned."
                    +" If the processor has no incoming connections then this parameter has no effect."
            )
            .required(true).expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(VALID_FAIL_STRATEGY)
            .defaultValue(VALID_FAIL_STRATEGY[1])
            .build();
    public static  Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All Failure records will be sent to this Relationship if configured to do so")
            .build();
    public static  Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All records will be sent to this Relationship if configured to do so, unless a failure occurs")
            .build();

    private List<PropertyDescriptor> properties ;

    private Set<Relationship> relationships = new HashSet<>(Arrays.asList(new Relationship[] {SUCCESS, FAILURE}));

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptor = new ArrayList<>();
        descriptor.add(FAIL_STRATEGY);
        this.properties = descriptor;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        if(!propertyDescriptorName.equals(FAIL_STRATEGY.getName())) {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .required(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                    .dynamic(true)
                    .build();
        }
        return FAIL_STRATEGY;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if(!descriptor.getName().equals(FAIL_STRATEGY.getName())){
            //根据配置内容 追加Relationship关系
            if(null != newValue && null == getRelationship(descriptor.getName())){
                if(null == getRelationship(descriptor.getName())){
                    relationships.add(new Relationship.Builder()
                            .name(descriptor.getName())
                            .description("use rule push data to" + descriptor.getName().toUpperCase() + ".")
                            .build());
                }
            }
            //当属性参数输入有值且不为空值时，进行规则组装
            if(!StringUtils.isEmpty(newValue)){
                Ruler rule = new FieldValueRuler();
                rule.initRule(newValue);
                if (rule.isAvailable()) {
                    distributers.put(getRelationship(descriptor.getName()),rule);
                    getLogger().info("进行了规则校验且追加成功");
                }else{
                    getLogger().error("规则"+newValue+"语法不合规，请重新填写");
                }
            }
            if(null == newValue && null != getRelationship(descriptor.getName())){
                relationships.remove(getRelationship(descriptor.getName()));
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        boolean toFailureOnError = VALID_FAIL_STRATEGY[1].equals(processContext.getProperty(FAIL_STRATEGY).getValue());
        //获取前置管道数据流
        String originalDataStr = getProcessSessionFlowDatas(processSession);
        try {
            //根据规则进行数据的分发组织
            Map<Relationship, String> distributeResult = getRelationshipByDistribute(originalDataStr);
            for (Relationship relationship : distributeResult.keySet()) {
                sendDataToProcessSession(processSession, distributeResult.get(relationship), relationship);
            }
        }catch (Exception e){
            getLogger().error(e.getMessage());
            if (toFailureOnError) {
                //transfer all received to failure with two new attributes: ERROR_MESSAGE and ERROR_STACKTRACE.
                sendDataToProcessSession(processSession,originalDataStr,FAILURE);
            } else {
                processSession.rollback(true);
            }
        }
    }

    /**
     * 根据属性的name值获取对应的Relationship关系
     * @param name PropertyDescriptor的name值
     * @return Relationship 关系
     */
    public Relationship getRelationship(String name){
        for (Relationship r : relationships){
            if (r.getName().equals(name)){
                return r;
            }
        }
        return null;
    }

    /**
     * 将数据匹配到对应的管道中
     * @param datas 待拆分数据
     * @return 分发管道+对应需要发送的数据
     */
    private Map<Relationship,String> getRelationshipByDistribute(String datas){
        Map<Relationship,String> result = new HashMap<>();
        for(Relationship relationship:distributers.keySet()){
            result.put(relationship,distributers.get(relationship).accordRule(datas));
        }
        return result;
    }


    /**
     * 获取processSession中的flowfile的数据
     * @param processSession session
     * @return String 数据集
     */
    private String getProcessSessionFlowDatas(ProcessSession processSession){
        StringBuffer result = new StringBuffer();
        FlowFile flowFile = processSession.get();
        processSession.read(flowFile, in -> {
            try{
                String json = IOUtils.toString(in);
                result.append(json);
            }catch(Exception ex){
                ex.printStackTrace();
                getLogger().error("Failed to read json string.");
            }
        });
        processSession.remove(flowFile);
        return result.toString();
    }

    /**
     * 输出数据至指定的管道
     * @param processSession session
     * @param datas 数据
     * @param relationship 输送关系
     */
    private void sendDataToProcessSession(ProcessSession processSession,String datas,Relationship relationship){
        if(!StringUtils.isEmpty(datas)) {
            FlowFile flowFile = processSession.create();
            flowFile = processSession.write(flowFile, out -> out.write(datas.getBytes()));
            processSession.transfer(flowFile, relationship);
        }
    }

}
