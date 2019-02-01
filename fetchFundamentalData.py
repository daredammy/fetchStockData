# -#- coding: utf-8 -#-
"""
    File name: fetchFundamentalData.py
    Author: Damilola Dare-Olipede
    Python Version: 3.6
    Use:
        To fetch fundamental stock data from fianance.yahoo.com
    HISTORY:
     #   31-JAN-2019 V2.0 Reorganized the code, comments (DD)
     #   14-SEP-2018 V1.2 Added logging (DD)
     #   12-SEP-2018 V1.1 Created exportToCsv() function (DD)
     #   17-AUG-2018 V1.0 Original File Damilola Dare-Olipede (DD)
"""

import time, datetime, requests, bs4, os, csv, re, webbrowser, pickle, math, threading, logging
from platform import system

inputFile = "NASDAQ_exchange.csv" #csv file with stock symbols in first column see repository
logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - %(levelname)s- %(message)s')
#logging.disable()
timeNow = str(datetime.datetime.now().strftime('%m-%d %H_%M_%S'))
outputFileName = "Fundamentals_yahoo %s.csv" % (timeNow)

#Fundamental data to collect from yahoo finance- https://finance.yahoo.com/quote/AAPL/key-statistics?p=AAPL
dataToCollect = ["Dividend Date", "Ex-Dividend Date", "Last Split Date", "Last Split Factor",
                 "Diluted EPS", "PEG Ratio", "Book Value Per Share", "EBITDA", "Price/Sales",
                 "Price/Book", "Forward P/E", "Revenue", "Shares Short",
                 "Market Cap", "Float", "Shares Outstanding", "Profit Margin" ,
                 "Operating Margin", "Return on Assets", "Return on Equity",
                 "Quarterly Revenue Growth", "Gross Profit", "Quarterly Earnings Growth",
                 "% Held by Insiders", "% Held by Institutions", "Shares Short",
                 "Forward Annual Dividend Rate", "Operating Cash Flow", "Levered Free Cash Flow","Beta", ]

#Fundamental data with unique regex patterns
specialCollect = ["EBITDA","Revenue" ]


# createTickersDictionary - Create a dictionary with the stock tickers as keys
# USE:
#    createTickersDictionary(inputFileName)
# WHERE:
#    inputFileName = path to csv file with stock symbols in first column
# NOTES:
#
# HISTORY:
#   17-AUG-2018 V1.0 Original File Damilola Dare-Olipede (DD)
def createTickersDictionary(inputFileName):
    tickersD = dict()
    with open(inputFileName ) as f:
        csvDataReader = csv.reader(f)
        for i,rows in enumerate  (list(csvDataReader),0):
            if i>0:
                tickersD.setdefault(rows[0], [])
    return tickersD

# fetchData - fetch data from finance.yahoo.com
# USE:
#    fetchData(strtTckIndex, TckIndex, data2Collect, dataSpecialRegex , tickersDictionary)
# WHERE:
#    strtTckIndex = starting index of tickers to fetch
#    TckIndex = ending index of tickers to fetch
#    data2Collect = fundamental data to be collected
#    dataSpecialRegex = fundamental data with unique regex

# NOTES:
#
# HISTORY:
#   17-AUG-2018 V1.0 Original File Damilola Dare-Olipede (DD)
def fetchData(strtTckIndex, TckIndex, data2Collect, dataSpecialRegex , tickersDictionary):
   for i,ticker in enumerate (list(tickersDictionary.keys())[strtTckIndex:TckIndex],0):
        webLink =  "https://finance.yahoo.com/quote/"+ ticker + "/key-statistics?p=" + ticker
        res = requests.get(webLink)
        soup = bs4.BeautifulSoup(res.text, features="html.parser")
        #Use beautiful soup to find regex pattern
        for item in data2Collect:
            if item not in dataSpecialRegex :
                preceed = soup.find (["tr","td","span"], string= re.compile(item))
            else:
                preceed = soup.find (["tr","td","span"], string= item)
            try:
                value = preceed.findNext ('td').getText()
                if (value == '\u221e'): # handle infinity character Unicode
                    value = "Infinity"
            except (AttributeError,UnicodeEncodeError):
                value ="N/A"
                logging.debug ('failed to collect (%s) for (%s)'% (item, ticker))
            tickerList= list (tickersDictionary.get(ticker))
            tickerList.append (value)
            tickersDictionary.update ({ticker: tickerList})



# exportToCsv - export Collected Fundamental Stock Data to CSV
# USE:
#    exportToCsv(inputFileName, destFileName, data2Collect, tickersDictionary):
# WHERE:
#    destFileName = file path of output csv file
# NOTES:
#
# HISTORY:
#   12-SEP-2018 V1.1 Original File Damilola Dare-Olipede (DD)
def exportToCsv(inputFileName, destFileName, data2Collect, tickersDictionary):
    outputFile = open(destFileName, 'w', newline='')
    outputWriter = csv.writer(outputFile)
    with open(inputFileName) as f:
       csvDataReader = csv.reader(f)
       for i,rows in enumerate  (list(csvDataReader),0):
           if i==0:
               rows.extend(data2Collect) # extend first row with fundamental data columns
               try:
                   outputWriter.writerow(rows)
               except UnicodeEncodeError:
                   outputWriter.writerow([''])
           for ticker in tickersDictionary.keys():
               if rows[0] == ticker:
                   rows.extend(tickersDictionary.get(ticker))
                   outputWriter.writerow(rows)
    outputFile.close()



# Create multiple threads
Threads = []
tickerPartitions = 100 ; # divide the tickers into 'x' number of places, used for thread grouping
tickersD = createTickersDictionary(inputFile)
noOfThreads = len(tickersD.keys()) / tickerPartitions
for i in range(0, len(tickersD.keys()), tickerPartitions):
   Thread = threading.Thread(target= fetchData, args=(i, i + tickerPartitions-1, dataToCollect, specialCollect, tickersD))
   Threads.append(Thread)
   Thread.start()
for thread in Threads:
   thread.join()
logging.debug('Threads Complete')
exportToCsv(inputFile, outputFileName, dataToCollect, tickersD)
logging.debug('CSV file created check- %s\\%s'% (os.getcwd(), outputFileName))
