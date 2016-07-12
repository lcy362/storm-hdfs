package org.apache.storm.hdfs.bolt.rotation;

import backtype.storm.tuple.Tuple;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

/**
 * Created by lcy on 2016/7/11.
 * do filerotation everyday, similar as log4j dailyretation
 */
public class DailyRotationPolicy implements FileRotationPolicy {
    private String currentDate;
    private String dateFormat = "yyyy-MM-dd";

    public DailyRotationPolicy() {
        DateTime dt = new DateTime();
        currentDate = dt.toString(dateFormat);
    }
    @Override
    public boolean mark(Tuple tuple, long offset) {
        DateTime dt = new DateTime();
        String nowdate = dt.toString(dateFormat);
        if (StringUtils.equals(currentDate, nowdate)) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void reset() {
        DateTime dt = new DateTime();
        currentDate = dt.toString(dateFormat);
    }
}
