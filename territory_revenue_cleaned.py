#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd

# Load Raw Data

# Load raw data from CLIMS4.
###########################################
#### Change the file path if necessary ####
###########################################
rawData = pd.read_excel(r'Z:\August13\SalesDashboardList_2019-08-13 (Jul 1- Aug 11).xlsx' 
                        )

# convert data type of "CreatedDate" to datetime
rawData['CreatedDate'] = pd.to_datetime(rawData.CreatedDate)
# make a copy for raw data
rawDataCopy = rawData.copy()

## Step1: Clean order status

### For direct order
# take out 'Cloning and Mutagenesis', 'Gene Synthesis', 'Next Gen. Sequencing', which are quote-based orders
# for order status of direct order services, take out 'Cart', 'Discard', 'Draft', 'Pending Quote', 'Ready To Order'
rawDataDirect = rawDataCopy[(~rawDataCopy['LineOfBusinessType'].isin(['Cloning and Mutagenesis', 'Gene Synthesis', 
                             'Next Gen. Sequencing', 'Molecular Genetics', 'Regulatory']))
                         &(~rawDataCopy['OrderStatus'].isin(['Cart', 'Discard', 'Draft', 'Pending Quote', 'Ready To Order']))]

### For quote-based order
# for quote-based order, firstly take out only 'Draft', 'Pending Quote'
rawDataQuote = rawDataCopy[(rawDataCopy['LineOfBusinessType'].isin(['Cloning and Mutagenesis', 'Gene Synthesis', 
                            'Next Gen. Sequencing', 'Molecular Genetics', 'Regulatory']))
                         &(~rawDataCopy['OrderStatus'].isin(['Draft', 'Pending Quote']))]
# for quote issued order, take out 'cart', 'ready to order'
rawDataQuote = rawDataQuote.drop(rawDataQuote[(rawDataQuote.QuotationNumber.notna()) & 
                                              (rawDataCopy['OrderStatus'].isin(['Cart', 'Ready To Order']))].index)

# merge direct order and quote-based order
rawData1 = pd.concat([rawDataDirect, rawDataQuote]).sort_index()

## Step 2: Clean Territory

# Drop customers from countries other than the United States
rawData2 = rawData1[(rawData['Territory']!='20A')&(rawData['Territory']!='20B')&(rawData['Territory']!='20C')&(rawData['Territory']!='20D')
                  &(rawData['Territory']!='21A')&(rawData['Territory']!='21B')&(rawData['Territory']!='21C')&(rawData['Territory']!='21D')
                  &(rawData['Territory']!='22A')&(rawData['Territory']!='22B')&(rawData['Territory']!='22C')&(rawData['Territory']!='22D')
                  &(rawData['Territory']!='30A')&(rawData['Territory']!='EUBD')&(rawData['Territory']!='40A')]

## Step 3: Clean Billing Amount

# remove orders where billing amount is $0.0000
rawData2 = rawData2[rawData2['BillingAmount']!="$0.0000"]
# remove orders where billing amount contains "JPY.￥"
rawData2 = rawData2[~rawData2.BillingAmount.str.contains("JPY.￥")]
rawData2 = rawData2[~rawData2.BillingAmount.str.contains("£")]
rawData2 = rawData2[~rawData2.BillingAmount.str.contains("€")]

# convert AU$ to USD
# remove "AU$"
# convert string to float then time exchange rate (go check the latest rate)
# convert float to string
# add "$" back
rawData2.loc[rawData2.BillingAmount.str.contains("AU"), 'BillingAmount'] = rawData2.loc[rawData2.BillingAmount.str.contains("AU"),'BillingAmount'].apply(lambda x:"$"+str(np.round(float(x[3:])*0.69,decimals=4)))
# convert billing amount to float
rawData2['BillingAmount'] = rawData2['BillingAmount'].str[1:]
rawData2['BillingAmount'] = rawData2['BillingAmount'].apply(lambda x:np.round(float(x),decimals=4))

### Step 4: Drop GENEWIZ related records

rawData3 = rawData2[~rawData2.Institution.str.contains('GENEWIZ', na = False)]
rawData3 = rawData3[~rawData3.Institution.str.contains('genewiz', na = False)]

rawData3 = rawData3[~rawData3.CustomerEmail.str.contains('genewiz.test', na = False)]
rawData3 = rawData3[~rawData3.CustomerEmail.str.contains('genewiz.com', na = False)]

### Step 5: Change CM to GS

rawData4 = rawData3.copy()
# Change CM to GS
rawData4.loc[rawData4['LineOfBusinessType'] == 'Cloning and Mutagenesis', 'LineOfBusinessType'] = 'Gene Synthesis'


###########################################
#### Change the file path if necessary ####
###########################################
# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(r'Z:\August13\CleanedData.xlsx', engine='xlsxwriter')

# Write each dataframe to a different worksheet.
rawData.to_excel(writer, sheet_name='Raw', index=False)
rawData3.to_excel(writer, sheet_name='Cleaned Territory and Revenue', index=False)
rawData4.to_excel(writer, sheet_name='CM to GS (Ready)', index=False)

# Close the Pandas Excel writer and output the Excel file.
writer.save()
