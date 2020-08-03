package udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ParseOutlineSection extends UDF {
    public String evaluate(String line) {
        String outline = "";
        try {
            JSONArray jsonArray = new JSONArray(line);
            for (int i = 1; i < jsonArray.length(); i++) {
                JSONObject jo = new JSONObject(jsonArray.get(i).toString());
                outline = outline + jo.get("name").toString() + ",";
            }

            outline = outline.substring(0, outline.length() - 1);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return outline;
    }
}
