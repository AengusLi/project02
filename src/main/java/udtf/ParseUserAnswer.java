package udtf;

import bean.AnswerBean;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

public class ParseUserAnswer extends GenericUDTF {
    //输出结果为三列，每列均为String类型
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("question_id");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("useranswer");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("score");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String[] newline = new String[3];
        //参数，即一条答案串
        String line = args[0].toString();
        HashMap map = JSONObject.parseObject(line, HashMap.class);
        map.forEach((k, vJSON) -> {
            newline[0] = k.toString();
            AnswerBean answerBean = JSONObject.parseObject(vJSON.toString(), AnswerBean.class);
            newline[1] = answerBean.getUseranswer();
            newline[2] = answerBean.getScore();
            try {
                forward(newline);
            } catch (HiveException e) {
                e.printStackTrace();
            }
        });

    }

    @Override
    public void close() throws HiveException {

    }

    @Test
    public void test() throws JSONException {
        String[] newline = new String[3];
        String str1 = "{\"842\":{\"useranswer\":\"3407|3408|3410\",\"score\":0},\"846\":{\"useranswer\":\"3411\",\"score\":2},\"847\":{\"useranswer\":\"3499\",\"score\":2},\"848\":{\"useranswer\":\"3419\",\"score\":2},\"849\":{\"useranswer\":\"3423\",\"score\":2},\"850\":{\"useranswer\":\"3505\",\"score\":2},\"851\":{\"useranswer\":\"3433\",\"score\":2},\"854\":{\"useranswer\":\"3443\",\"score\":2},\"855\":{\"useranswer\":\"3448\",\"score\":2},\"856\":{\"useranswer\":\"3452|3453\",\"score\":2},\"857\":{\"useranswer\":\"3455|3456|3457\",\"score\":2},\"858\":{\"useranswer\":\"3459|3460|3461\",\"score\":0},\"859\":{\"useranswer\":\"3463|3464|3465\",\"score\":2},\"861\":{\"useranswer\":\"3472|3473\",\"score\":2},\"862\":{\"useranswer\":\"3477\",\"score\":2},\"866\":{\"useranswer\":\"gulp.task(\",\"score\":0},\"867\":{\"useranswer\":\"requirejs()\",\"score\":0},\"872\":{\"useranswer\":\"git push -v origin master\",\"score\":0},\"873\":{\"useranswer\":\"git pull origin master\",\"score\":0},\"875\":{\"useranswer\":\"mkdir\",\"score\":2}}";
        String str = "{\"842\":{\"useranswer\":\"3407|3408|3410\",\"score\":0}}";
        HashMap map = JSONObject.parseObject(str, HashMap.class);
        map.forEach((k, vJSON) -> {
            newline[0] = k.toString();
            AnswerBean answerBean = JSONObject.parseObject(vJSON.toString(), AnswerBean.class);
            newline[1] = answerBean.getUseranswer();
            newline[2] = answerBean.getScore();
        });
        for (String s : newline) {
            System.out.println(s);
        }
    }
}
