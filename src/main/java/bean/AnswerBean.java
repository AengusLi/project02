package bean;

public class AnswerBean {
    private String useranswer;
    private String score;

    public AnswerBean() {
    }

    public String getUseranswer() {
        return useranswer;
    }

    public void setUseranswer(String useranswer) {
        this.useranswer = useranswer;
    }

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return useranswer + "," + score;
    }
}
