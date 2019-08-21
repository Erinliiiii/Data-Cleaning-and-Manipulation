#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
from pandasql import sqldf
import datetime


# # Import clean data and historical list

######################################
#### 1. Update path of clean data ####
######################################
# load cleaned data
cleaned = pd.read_excel(r'Z:\August13\CleanedData.xlsx', 
                        sheet_name='CM to GS (Ready)')

cleaned2 = cleaned.copy()
cleaned2['lob'] = cleaned2['LineOfBusinessType']

# creating a column indicating short name
cleaned2.loc[cleaned2['LineOfBusinessType'] == 'Gene Synthesis', 'lob'] = 'gs'
cleaned2.loc[cleaned2['LineOfBusinessType'] == 'Molecular Genetics', 'lob'] = 'molgen'
cleaned2.loc[cleaned2['LineOfBusinessType'] == 'Next Gen. Sequencing', 'lob'] = 'ngs'
cleaned2.loc[cleaned2['LineOfBusinessType'] == 'Oligo Synthesis', 'lob'] = 'oligo'
cleaned2.loc[cleaned2['LineOfBusinessType'] == 'Plasmid DNA Prep.', 'lob'] = 'prep'
cleaned2.loc[cleaned2['LineOfBusinessType'] == 'Regulatory', 'lob'] = 'rs'
cleaned2.loc[cleaned2['LineOfBusinessType'] == 'Sanger Sequencing', 'lob'] = 'sanger'

# concat email and LoB
cleaned2['email_lob'] = cleaned2['CustomerEmail'] + '*' + cleaned2['lob']
# lowercase all the email address
cleaned2['email_lob'] = cleaned2['email_lob'].str.lower()
cleaned2['CustomerEmail'] = cleaned2['CustomerEmail'].str.lower()


###########################################
#### 2. Update path of historical list ####
###########################################
# load historical customer list
historical = pd.read_excel(r'Z:\Marketing Campaign Tracking Python\July21\Historical Customer List for Campaign Tracking_updated.xlsx',
                          sheet_name = 'No 2019 H2')

# change column name
historical.columns = [c.lower().replace(' ', '_') for c in historical.columns]
historical.rename(columns={'email':'CustomerEmail','email_+_lob':'email_lob'},inplace=True)


# ## Molgen / RS -- subset by LoB

# For select_time_lob function, you need to:
# 1. imput the complete file name of email open list
# 2. imput the email sent date in the format like: '07/30/2019'
# 3. imput the LoB types that should be tracked in the format like: ['Molecular Genetics', 'Regulatory']

###########################################################################################
#### 3. Update path of regional campaign email open lists in select_time_lob function; #### 
####    Modify the filter in function if necessary ########################################
###########################################################################################

# In the following function, the criteria for filtering the data is based on line of business. You can change the criteria from 'LineOfBusinessType' to 'OrderType', or other condition if necessary.
def select_time_lob(email_name, sent_date, lob):
    # import all email open lists
    path = r'Z:\August13\Regional'
    email_opened = pd.read_excel(path + '\\' + email_name + ".xlsx")  ### the first argument
    
    # convert column name to lower case and replace space with '_'
    email_opened.columns = [c.lower().replace(' ', '_') for c in email_opened.columns]
    
    # lowercase the email address
    email_opened['recipient'] = email_opened['recipient'].str.lower()
    
    # transfer sent_date to datetime type
    start_date = pd.to_datetime(sent_date)       ### the second argument
    end_date = start_date + datetime.timedelta(days = 31)
    
    # if the criteria to filter the data is based on line of business, you can change the 'LineOfBusinessType' to 'OrderType'
    ready = cleaned2.loc[cleaned2['LineOfBusinessType'].isin(lob), :]   ### the thrid argument  
    ready = ready.loc[(ready['CreatedDate']>=start_date)&(ready['CreatedDate']<=end_date), :]
    
    # inner join sales dashboard with email open list
    tmp = pd.merge(left=ready,right=email_opened[['recipient']],left_on='CustomerEmail',right_on='recipient',
                             how='inner').drop(columns=['recipient'])
    
    # get historical list
    # get the order list from 1/1/2019 to email sent date
    hist_2019 = cleaned2[cleaned2['CreatedDate'] < start_date]

    # concat customer list of 2016-2018 and list of 2019
    hist_list = pd.concat([historical[['CustomerEmail', 'email_lob']],hist_2019[['CustomerEmail', 'email_lob']]], 
                           ignore_index=True, sort =False)

    # change the column name for joining 
    hist_list.rename(columns={'CustomerEmail':'email', 'email_lob':'email_and_lob'}, inplace=True)
    hist_list['email'] = hist_list['email'].str.lower()
    hist_list['email_and_lob'] = hist_list['email_and_lob'].str.lower()

    # keep unique email in historial list to check if a custsomer is new 
    hist_list_email_de = hist_list.drop_duplicates(subset = 'email', keep = 'first')
    # keep unique email+LoB in historial list to check if a custsomer is new to LoB
    hist_list_lob_de = hist_list.drop_duplicates(subset = 'email_and_lob', keep = 'first')
    
    # check whether it's new to genewiz
    # left join flash_labeled and historical email list based on email
    labeled = pd.merge(left=tmp, 
                       right=hist_list_email_de[['email']],
                       left_on='CustomerEmail',
                       right_on='email',
                       how='left')

    # add a column indicating if it's new to company
    labeled['new_to_company'] = (labeled.email.isna()).astype(np.int)
    
    # left join flash_labeled and historical email+lob list based on email+lob
    labeled = pd.merge(left=labeled, 
                        right=hist_list_lob_de[['email_and_lob']],
                        left_on='email_lob',
                        right_on='email_and_lob',
                        how='left')

    # add a column indicating if it's new to lob
    labeled['new_to_lob'] = (labeled.email_and_lob.isna()).astype(np.int)

    # for new to company customers, convert value of new_to_lob to NA
    labeled.loc[labeled['new_to_company'] == 1, 'new_to_lob'] = np.nan
    
    return labeled

#################################################################################
#### 4. Before calling the function, update the email name, email sent date, ####
####    and the list of lob that you would like to track ########################
#################################################################################
    
regional_molgen_final = select_time_lob('RegionalMolGenSD+SFPromo30% off PCR+Sanger', '07/30/2019', 
                                        ['Molecular Genetics', 'Regulatory'])


# ## GS -- subset by priority

# For select_time_priority function, you need to:
# 1. imput the complete file name of email open list
# 2. imput the email sent date in the format like: '07/30/2019'
# 3. imput the priority types that should be tracked in the format like: ['PriorityGENE']

# The following function is basically the same as the previous one. The only difference is the filter has been changed from 'LineOfBusinessType' to 'Priority'.

def select_time_priority(email_name, sent_date, priority):
    # import all email open lists
    path = r'Z:\August13\Regional'
    email_opened = pd.read_excel(path + '\\' + email_name + ".xlsx")
    
    # convert column name to lower case and replace space with '_'
    email_opened.columns = [c.lower().replace(' ', '_') for c in email_opened.columns]
    
    # lowercase the email address
    email_opened['recipient'] = email_opened['recipient'].str.lower()
    
    # transfer sent_date to datetime type
    start_date = pd.to_datetime(sent_date)
    end_date = start_date + datetime.timedelta(days = 31)
    
    # the criteria is 'Priority' now
    ready = cleaned2.loc[cleaned2['Priority'].isin(priority), :]
    ready = ready.loc[(ready['CreatedDate']>=start_date)&(ready['CreatedDate']<=end_date), :]
    
    # inner join sales dashboard with email open list
    tmp = pd.merge(left=ready,right=email_opened[['recipient']],left_on='CustomerEmail',right_on='recipient',
                             how='inner').drop(columns=['recipient'])
    
    # get historical list
    # get the order list from 1/1/2019 to email sent date
    hist_2019 = cleaned2[cleaned2['CreatedDate'] < start_date]

    # concat customer list of 2016-2018 and list of 2019
    hist_list = pd.concat([historical[['CustomerEmail', 'email_lob']],hist_2019[['CustomerEmail', 'email_lob']]], 
                           ignore_index=True, sort =False)

    # change the column name for joining 
    hist_list.rename(columns={'CustomerEmail':'email', 'email_lob':'email_and_lob'}, inplace=True)
    hist_list['email'] = hist_list['email'].str.lower()
    hist_list['email_and_lob'] = hist_list['email_and_lob'].str.lower()

    # keep unique email in historial list to check if a custsomer is new 
    hist_list_email_de = hist_list.drop_duplicates(subset = 'email', keep = 'first')
    # keep unique email+LoB in historial list to check if a custsomer is new to LoB
    hist_list_lob_de = hist_list.drop_duplicates(subset = 'email_and_lob', keep = 'first')
    
    # check whether it's new to genewiz
    # left join flash_labeled and historical email list based on email
    labeled = pd.merge(left=tmp, 
                       right=hist_list_email_de[['email']],
                       left_on='CustomerEmail',
                       right_on='email',
                       how='left')

    # add a column indicating if it's new to company
    labeled['new_to_company'] = (labeled.email.isna()).astype(np.int)
    
    # left join flash_labeled and historical email+lob list based on email+lob
    labeled = pd.merge(left=labeled, 
                        right=hist_list_lob_de[['email_and_lob']],
                        left_on='email_lob',
                        right_on='email_and_lob',
                        how='left')

    # add a column indicating if it's new to lob
    labeled['new_to_lob'] = (labeled.email_and_lob.isna()).astype(np.int)

    # for new to company customers, convert value of new_to_lob to NA
    labeled.loc[labeled['new_to_company'] == 1, 'new_to_lob'] = np.nan
    
    return labeled

# call the function
regional_gs_final = select_time_priority('RegionalGSPromoD1 PriorityGENE PromoJuly 2019', '7/31/2019', ['PriorityGENE'])


# # Metrics Calculation

def get_metrics(email_final):
    final = email_final.copy()
    
    # number of order and revenue
    order_revenue_q = """
                        SELECT 
                        COUNT(CustomerEmail) AS `# of Order`,
                        SUM(BillingAmount) AS Revenue
                        FROM final;
                        """
    
    order_revenue = sqldf(order_revenue_q, locals())
    
    # revenue by customer status
    customer_status_q = """
    SELECT 
            COUNT(CASE WHEN new_to_company = 0 THEN CustomerEmail ELSE NULL END) AS Return_Order,
            SUM(CASE WHEN new_to_company = 0 THEN BillingAmount ELSE 0 END) AS Return_Revenue,
            COUNT(CASE WHEN new_to_company = 1 THEN CustomerEmail ELSE NULL END) AS NewToGWZ_Order,
            SUM(CASE WHEN new_to_company = 1 THEN BillingAmount ELSE 0 END) AS NewToGWZ_Revenue,
            COUNT(CASE WHEN new_to_lob = 1 THEN CustomerEmail ELSE NULL END) AS NewToLoB_Order,
            SUM(CASE WHEN new_to_lob = 1 THEN BillingAmount ELSE 0 END) AS NewToLoB_Revenue
    FROM final;
    """

    by_customer_status = sqldf(customer_status_q, locals())
    
    # new customer acquired
    new_customer_acquired_q = """
    SELECT 
            COUNT (DISTINCT(CASE WHEN new_to_company = 1 THEN CustomerEmail ELSE NULL END)) AS NewToGWZ,
            COUNT (DISTINCT(CASE WHEN new_to_lob = 1 THEN CustomerEmail ELSE NULL END)) AS NewToLoB
    FROM final;
    """

    new_customer_acquired = sqldf(new_customer_acquired_q, locals())
    
    # count number of unique customers
    unique_number_q = """
                    SELECT 
                    COUNT(DISTINCT(CustomerEmail)) AS '# of unique customers'
                    FROM final;
                """

    unique_number = sqldf(unique_number_q, locals())
    
    # combine all the metrics
    metrics = pd.concat([order_revenue, by_customer_status, new_customer_acquired, unique_number], axis = 1)
    
    return metrics

######################################################################
#### 5. Before calling the get_metrics function, change the input ####
####    to the name of table we get from the select_time function ####
######################################################################
regional_molgen_metrics = get_metrics(regional_molgen_final)
regional_gs_metrics = get_metrics(regional_gs_final)


# # New Customer list

def get_new_customer_list(email_final, email_short_name):
    final = email_final.copy()
    
    # new to company
    new_to_company_list = final.loc[final['new_to_company'] == 1, 
                                    ['CustomerEmail', 'UserName', 'Institution', 'lob']]
    new_to_company_list.insert(loc=0, column='New Customer Type', value='New to GENEWIZ')

    # new to lob
    new_to_lob_list = final.loc[final['new_to_lob']==1,
                                ['CustomerEmail', 'UserName', 'Institution', 'lob']]
    new_to_lob_list.insert(loc=0, column='New Customer Type', value='New to ' + new_to_lob_list.lob)
    new_to_lob_list.loc[:,'lob'] = 'N/A'

    new_customer_list_complete = pd.concat([new_to_company_list, new_to_lob_list], 
                                           ignore_index = True, sort = False)
    new_customer_list_complete = new_customer_list_complete.rename(columns={'CustomerEmail':'Customer Email', 'UserName':'Customer Name',
                                              'lob':'First Order LoB'})

    new_customer_list_complete = new_customer_list_complete.drop_duplicates(subset = None, keep = 'first')
    new_customer_list_complete.insert(loc=4, column='Campaign Source', value=email_short_name)
    
    return new_customer_list_complete

#######################################################################
#### 6. Before calling get_new_customer_list function, update name ####
####    of final result, and the corresponding email short name    ####
#######################################################################

regional_molgen_new_customer = get_new_customer_list(regional_molgen_final, 'Jul MolGen/RS Regional')
regional_gs_new_customer = get_new_customer_list(regional_gs_final, 'Jul GS Regional')


# # Export results

###################################################################
#### 7. Update path and file name before exporting the results ####
###################################################################

# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(r'Z:\August13\Regional\Regional Final.xlsx', engine='xlsxwriter')
# Write each dataframe to a different worksheet.
regional_molgen_final.to_excel(writer, sheet_name='MolGen', index=True)
regional_gs_final.to_excel(writer, sheet_name='GS', index=True)
# Close the Pandas Excel writer and output the Excel file.
writer.save()

# export metrics
# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(r'Z:\August13\Regional\Regional Metrics.xlsx', engine='xlsxwriter')
# Write each dataframe to a different worksheet.
regional_molgen_metrics.to_excel(writer, sheet_name='MolGen', index=True)
regional_gs_metrics.to_excel(writer, sheet_name='GS', index=True)
# Close the Pandas Excel writer and output the Excel file.
writer.save()

# export new customer lists
writer = pd.ExcelWriter(r'Z:\August13\Regional\Regional New Customer List.xlsx', engine='xlsxwriter')
regional_molgen_new_customer.to_excel(writer, sheet_name='MolGen', index=False)
regional_gs_new_customer.to_excel(writer, sheet_name='GS', index=False)
writer.save()

