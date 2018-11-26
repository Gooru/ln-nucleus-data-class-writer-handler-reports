package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BaseUtil {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseUtil.class);

    public static String UTCToLocale(Long strUtcDate, String timeZone) {

        String strLocaleDate = null;
        try {
            Long epohTime = strUtcDate;
            Date utcDate = new Date(epohTime);

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
            String strUTCDate = simpleDateFormat.format(utcDate);
            simpleDateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));

            strLocaleDate = simpleDateFormat.format(utcDate);

            LOGGER.debug("UTC Date String: " + strUTCDate);
            LOGGER.debug("Locale Date String: " + strLocaleDate);

        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }

        return strLocaleDate;
    }
    
}
