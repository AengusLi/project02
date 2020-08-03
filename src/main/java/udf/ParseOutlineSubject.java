package udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;


public class ParseOutlineSubject extends UDF {
    public String evaluate(String line) {
        String subject = "";
        try {
            JSONArray jsonArray = new JSONArray(line);
            JSONObject jsonObject = new JSONObject(jsonArray.get(0).toString());
            subject = jsonObject.get("name").toString();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return subject;
    }

    @Test
    public void test() throws JSONException {
        String jsonArr = "[{\"id\":50,\"name\":\"Python\",\"pId\":0},{\"id\":77,\"name\":\"web前端\",\"pId\":50},{\"id\":78,\"name\":\"Linux&数据库\",\"pId\":50}]";
        System.out.println(evaluate(jsonArr));
    }
}
