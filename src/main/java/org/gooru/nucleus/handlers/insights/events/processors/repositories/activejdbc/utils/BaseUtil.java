package org.gooru.nucleus.handlers.insights.events.processors.repositories.activejdbc.utils;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.TimeZone;
import org.gooru.nucleus.handlers.insights.events.constants.GEPConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;

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

  public static String UTCToLocale(Date utcDate, String timeZone) {

    String strLocaleDate = null;
    try {
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

  public static String UTCToLocale(String utcStringDate, String timeZone) {
    String strLocaleDate = null;
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
    try {
      Date date = simpleDateFormat.parse(utcStringDate);
      String strUTCDate = simpleDateFormat.format(date);
      simpleDateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
      strLocaleDate = simpleDateFormat.format(date);

      LOGGER.debug("UTC Date String: " + strUTCDate);
      LOGGER.debug("Locale Date String: " + strLocaleDate);

    } catch (Exception e) {
      LOGGER.error(e.getMessage());
    }
    return strLocaleDate;
  }

  public static String UTCDate(Long strUtcDate) {

    String strUTCDate = null;
    try {
      Long epohTime = strUtcDate;
      Date utcDate = new Date(epohTime);

      SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
      simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
      strUTCDate = simpleDateFormat.format(utcDate);
      LOGGER.debug("UTC Date String: " + strUTCDate);

    } catch (Exception e) {
      LOGGER.error(e.getMessage());
    }

    return strUTCDate;
  }
  
  public static String setBase64EncodedAdditionalContext(Long dcaContentId) {
    String base64encodedString;
    try {
      JsonObject additionalContext = new JsonObject();
      additionalContext.put(GEPConstants.DCA_CONTENT_ID, dcaContentId);
      base64encodedString = Base64.getEncoder().encodeToString(
          additionalContext.toString().getBytes(GEPConstants.UTF8));
    } catch (UnsupportedEncodingException e) {
      LOGGER.error("Error while encoding additionalContext", e);
      return null;
    }
    return base64encodedString;
  }
}
