package dto;

public class FraudPredictionDTO {
    private double score;

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "dto.FraudPredictionDTO{" +
                "score=" + score +
                '}';
    }
}
