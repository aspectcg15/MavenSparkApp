package org.morgan.test.MavenSparkApp;

import java.util.List;
import org.apache.spark.mllib.recommendation.Rating;

public interface UpdateSQL {

	public void UpdateSQL(List<Rating> sortedRating);

}