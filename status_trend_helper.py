import pandas as pd 
import datetime as datetime
import numpy as np
import logging
import pymannkendall as mk

class TrendThreshold(object):
  """
  Trend Threshold object for trend calculation. 'timestamp_of_status' and 'value' are to custom collumn names. 
  These need to be change to your table collumn names
  :param avg_period: An average period in datapoints to calculated a moving avg. for Trending data is in- or decreasing
  :param avg_period_short: A more agile mva for detecting spikes in data
  :param threshold: Deviation that a spike may have in percentage
  :param runSchedulesBack: Scheduled datapoint to go back to detect spikes in data
  :param debug: Default false. Set to true if testing locally
  """

  def __init__(self, avg_period=7, avg_period_short=3, threshold=10, runSchedulesBack=15, debug=False):
    self.avg_period = avg_period
    self.avg_period_short = avg_period_short
    self.threshold = threshold
    self.runSchedulesBack = runSchedulesBack
    self.debug = debug

  def analyse_data(self, df):
    """
    Analyse dataframe of table history data to form mva and upper/lower limits
    :param df:
    :return upper_threshold, lower_threshold, moving_averages, moving_averages_short, result.trend:
    """
    i = 0
    moving_averages = []
    moving_averages_short = []
    upper_threshold = []
    lower_threshold = []
    try:
      while i <= len(df):
        window_dict = df["value"][i: i + self.avg_period]
        window_average = round(sum(window_dict) / self.avg_period, 2)
        moving_averages.append(window_average)

        window_dict = df["value"][i: i + self.avg_period_short]
        window_average_short = round(sum(window_dict) / self.avg_period_short, 2)
        moving_averages_short.append(window_average_short)
        upper_threshold.append(window_average_short + (window_average_short * self.threshold) / 100)
        lower_threshold.append(window_average_short - (window_average_short * self.threshold) / 100)

        i = i + 1

      del moving_averages_short[-self.avg_period_short:]
      del moving_averages[-self.avg_period:]
      del lower_threshold[-self.avg_period_short:]
      del upper_threshold[-self.avg_period_short:]

      """
      Cause of the prior deleting of bad data new forecasted data based on previous values are calculated and added.
      """
      for i in range(1, (self.avg_period_short + 1)):
        delta_forecast = round(sum(moving_averages_short[-self.avg_period_short:]) / self.avg_period_short)
        forecast = moving_averages_short[-1] - delta_forecast
        moving_averages_short.append(moving_averages_short[-1] + forecast)
        upper_threshold.append(moving_averages_short[-1] + (moving_averages_short[-1] * self.threshold) / 100)
        lower_threshold.append(moving_averages_short[-1] - (moving_averages_short[-1] * self.threshold) / 100)

      for i in range(1, (self.avg_period + 1)):
        delta_forecast = round(sum(moving_averages_short[-self.avg_period_short:]) / self.avg_period_short)
        forecast = moving_averages_short[-1] - delta_forecast
        moving_averages.append(moving_averages[-1] + forecast)

      trend_status = df["value"][-(self.avg_period_short + self.avg_period):].values.tolist()
      result = mk.original_test(trend_status)

      return upper_threshold, lower_threshold, moving_averages, moving_averages_short, result.trend
    except IndexError:
      if self.debug:
        print("Not enough datapoint to calculate trend. Adjust datapoints or let system create the missing datapoints")
        exit(1)
      else:
        logging.info(
          "Not enough datapoint to calculate trend. Adjust datapoints or let system create the missing datapoints")

  def spike_detection(self, dataframe=None):
    """
    Detect spike from analysed data and log if spike has been detected.
    :param dataframe:
    :return returningDataframe, spikeValue, trend: Three variables are returned, 1st is a dataframe containing time, value and thresholds. 2nd is the diviation from the trend and the last is the status of the trend
    """
    timeValue = []
    thresholdIndex = 0
    spikeValue = 0
    try:
      df = pd.DataFrame(dataframe)
      (upper_threshold, lower_threshold, moving_averages, moving_averages_short, trend) = self.analyse_data(df)
      returningDataframe = df
      returningDataframe["timestamp_of_status"] = df["timestamp_of_status"].astype(np.int64)
      returningDataframe["upperThreshold"] = pd.Series(upper_threshold)
      returningDataframe["lowerThreshold"] = pd.Series(lower_threshold)

      data = df[-self.runSchedulesBack:]
      upperThreshold = upper_threshold[-self.runSchedulesBack:]
      lowerThreshold = lower_threshold[-self.runSchedulesBack:]
      spike = False
      for time, value in zip(data["timestamp_of_status"], data["value"]):
        if value > upperThreshold[thresholdIndex]:
          spike = True
          timeValue = [time, value, upperThreshold[thresholdIndex], trend, "high increase in data"]
        if value < lowerThreshold[thresholdIndex]:
          spike = True
          timeValue = [time, value, lowerThreshold[thresholdIndex], trend, "possible missing data"]
        thresholdIndex += 1
      if spike:
        if self.debug:
          print(
            f"Spike detected - Trend: {timeValue[3]} - but {timeValue[2]} at {datetime.datetime.strptime(str(timeValue[0]).split('.')[0], '%Y-%m-%d %H:%M:%S')} with value: {timeValue[1]}")
          print(timeValue[1] - timeValue[2])
        else:
          logging.info(
            f"Spike detected - Trend: {timeValue[3]} - but {timeValue[2]} at {datetime.datetime.strptime(str(timeValue[0]).split('.')[0], '%Y-%m-%d %H:%M:%S')} with value: {timeValue[1]}")
          spikeValue = round(timeValue[1] - timeValue[2])

      return returningDataframe, spikeValue, trend
    except:
      logging.info("Error in calculating trend for table")